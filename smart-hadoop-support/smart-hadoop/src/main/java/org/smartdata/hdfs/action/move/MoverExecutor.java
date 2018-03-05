/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs.action.move;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.model.action.FileMovePlan;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A light-weight executor for Mover.
 */
public class MoverExecutor {
  static final Logger LOG = LoggerFactory.getLogger(MoverExecutor.class);

  private Configuration conf;
  private URI namenode;
  private String fileName;
  private NameNodeConnector nnc;
  private DFSClient dfsClient;
  private SaslDataTransferClient saslClient;

  private int concurrentMoves;
  private int maxConcurrentMoves;
  private int maxConcurrentMovesPerInst;
  private int maxRetryTimes;
  private ExecutorService moveExecutor;
  private List<ReplicaMove> allMoves;

  private Map<Long, Block> sourceBlockMap;
  private Map<String, DatanodeInfo> sourceDatanodeMap;
  private MoverStatus status;
  private List<LocatedBlock> locatedBlocks;

  private static AtomicInteger instances = new AtomicInteger(0);

  public MoverExecutor(MoverStatus status, Configuration conf,
      int maxRetryTimes, int maxConcurrentMoves) {
    this.status = status;
    this.conf = conf;
    this.maxRetryTimes = maxRetryTimes;
    this.maxConcurrentMoves = maxConcurrentMoves;
    maxConcurrentMovesPerInst = conf.getInt(
        SmartConfKeys.SMART_CMDLET_MOVER_MAX_CONCURRENT_BLOCKS_PER_SRV_INST_KEY,
        SmartConfKeys.SMART_CMDLET_MOVER_MAX_CONCURRENT_BLOCKS_PER_SRV_INST_DEFAULT);
  }

  /**
   * Execute a move action providing the schedule plan
   * @param plan the schedule plan of mover
   * @return number of failed moves
   * @throws Exception
   */
  public int executeMove(FileMovePlan plan, PrintStream resultOs, PrintStream logOs) throws Exception {
    if (plan == null) {
      throw new RuntimeException("Schedule plan for mover is null");
    }

    init(plan);

    HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileName);
    if (fileStatus == null) {
      throw new RuntimeException("File does not exist.");
    }

    if (fileStatus.isDir()) {
      throw new RuntimeException("File path is a directory.");
    }

    if (fileStatus.getLen() < plan.getFileLength()) {
      throw new RuntimeException("File has been changed after this action generated.");
    }

    locatedBlocks = dfsClient.getLocatedBlocks(fileName, 0, plan.getFileLength()).getLocatedBlocks();

    parseSchedulePlan(plan);

    concurrentMoves = allMoves.size() >= maxConcurrentMoves ? maxConcurrentMoves : allMoves.size();
    moveExecutor = Executors.newFixedThreadPool(concurrentMoves);
    try {
      instances.incrementAndGet();
      return doMove(resultOs, logOs);
    } finally {
      instances.decrementAndGet();
      moveExecutor.shutdown();
      moveExecutor = null;
    }
  }

  /**
   * Execute a move action providing the schedule plan.
   *
   * @param resultOs
   * @param logOs
   * @return
   * @throws Exception
   */
  public int doMove(PrintStream resultOs, PrintStream logOs) throws Exception {
    for (int retryTimes = 0; retryTimes < maxRetryTimes; retryTimes ++) {
      final AtomicInteger running = new AtomicInteger(0);
      for (final ReplicaMove replicaMove : allMoves) {
        moveExecutor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              running.incrementAndGet();
              replicaMove.run();
            } finally {
              running.decrementAndGet();
            }
          }
        });

        if (maxConcurrentMovesPerInst != 0) {
          while (running.get() > (maxConcurrentMovesPerInst * 1.0 / instances.get())) {
            Thread.sleep(50);
          }
        }
      }

      int sleeped = 0;
      int[] stat = new int[2];
      while (true) {
        ReplicaMove.countStatus(allMoves, stat);
        if (stat[0] == allMoves.size()) {
          status.increaseMovedBlocks(stat[1]);
          break;
        }
        Thread.sleep(10);
        sleeped += 10;
      }

      int remaining = ReplicaMove.refreshMoverList(allMoves);
      if (allMoves.size() == 0) {
        LOG.info("{} succeeded", this);
        return 0;
      }
      if (logOs != null) {
        logOs.println(String.format("The %d/%d retry, remaining = %d",
            retryTimes + 1, maxRetryTimes, remaining));
      }
      LOG.debug("{} : {} moves failed, start a new iteration", this, remaining);
      if (sleeped < 1000) {
        Thread.sleep(1000 - sleeped);
      }
    }
    int failedMoves = ReplicaMove.failedMoves(allMoves);
    LOG.info("{} : failed with {} moves", this, failedMoves);
    return failedMoves;
  }

  @VisibleForTesting
  public int executeMove(FileMovePlan plan) throws Exception {
    return executeMove(plan, null, null);
  }

  @Override
  public String toString() {
    return "MoverExecutor <" + namenode + ":" + fileName + ">";
  }

  private void init(FileMovePlan plan) throws IOException {
    this.namenode = plan.getNamenode();
    this.fileName = plan.getFileName();
    this.nnc = new NameNodeConnector(namenode, conf);
    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);
    dfsClient = nnc.getDistributedFileSystem().getClient();
    allMoves = new ArrayList<>();
  }

  private void parseSchedulePlan(FileMovePlan plan) throws IOException {
    generateSourceMap();

    List<String> sourceUuids = plan.getSourceUuids();
    List<String> sourceStorageTypes = plan.getSourceStoragetypes();
    List<String> targetIpAddrs = plan.getTargetIpAddrs();
    List<Integer> targetXferPorts = plan.getTargetXferPorts();
    List<String> targetStorageTypes = plan.getTargetStorageTypes();
    List<Long> blockIds = plan.getBlockIds();

    for (int planIndex = 0; planIndex < blockIds.size(); planIndex ++) {
      // build block
      Block block = sourceBlockMap.get(blockIds.get(planIndex));
      // build source
      DatanodeInfo sourceDatanode = sourceDatanodeMap.get(sourceUuids.get(planIndex));
      StorageGroup source = new StorageGroup(sourceDatanode, sourceStorageTypes.get(planIndex));
      //build target
      DatanodeInfo targetDatanode = CompatibilityHelperLoader.getHelper()
          .newDatanodeInfo(targetIpAddrs.get(planIndex), targetXferPorts.get(planIndex));
      StorageGroup target = new StorageGroup(targetDatanode, targetStorageTypes.get(planIndex));
      // generate single move
      ReplicaMove replicaMove = new ReplicaMove(block, source, target, nnc, saslClient);
      allMoves.add(replicaMove);
    }
  }

  private void generateSourceMap() throws IOException {
    sourceBlockMap = new HashMap<>();
    sourceDatanodeMap = new HashMap<>();
    for (LocatedBlock locatedBlock : locatedBlocks) {
      sourceBlockMap.put(locatedBlock.getBlock().getBlockId(), locatedBlock.getBlock().getLocalBlock());
      for (DatanodeInfo datanodeInfo : locatedBlock.getLocations()) {
        sourceDatanodeMap.put(datanodeInfo.getDatanodeUuid(), datanodeInfo);
      }
    }
  }
}
