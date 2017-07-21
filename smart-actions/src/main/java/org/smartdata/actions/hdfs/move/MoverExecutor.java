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
package org.smartdata.actions.hdfs.move;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

/**
 * A light-weight executor for Mover.
 */
public class MoverExecutor {
  static final Logger LOG = LoggerFactory.getLogger(MoverExecutor.class);

  private Configuration conf;
  private URI namenode;
  private String fileName;
  private NameNodeConnector nnc;
  private SaslDataTransferClient saslClient;

  private int maxConcurrentMoves;
  private ExecutorService moveExecutor;
  private List<SingleMoveStatus> allStatus;
  private List<SingleMove> allMoves;

  private Map<Long, Block> sourceBlockMap;
  private Map<String, DatanodeInfo> sourceDatanodeMap;

  public MoverExecutor(Configuration conf, int maxConcurrentMoves) {
    this.conf = conf;
    this.maxConcurrentMoves = maxConcurrentMoves;
  }

  // Execute a move action providing source and target
  // TODO: temporarily make use of Dispatcher, may need refactor
  public int executeMove(SchedulePlan plan) throws Exception {
    parseSchedulePlan(plan);
    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);
    moveExecutor = Executors.newFixedThreadPool(maxConcurrentMoves);
    for (SingleMove singleMove : allMoves) {
      moveExecutor.execute(singleMove);
    }
    while (!SingleMoveStatus.allMoveFinished(allStatus)) {
      Thread.sleep(1000);
    }
    return SingleMoveStatus.failedMoves(allStatus);
  }

  private void parseSchedulePlan(SchedulePlan plan) throws IOException {
    if (plan == null) {
      throw new RuntimeException("Schedule plan for mover is null");
    }
    this.namenode = plan.getNamenode();
    this.fileName = plan.getFileName();
    this.nnc = new NameNodeConnector(namenode, conf);
    allMoves = new ArrayList<>();
    allStatus = new ArrayList<>();

    generateSourceMap();

    List<String> sourceUuids = plan.getSourceUuids();
    List<StorageType> sourceStorageTypes = plan.getSourceStoragetypes();
    List<String> targetIpAddrs = plan.getTargetIpAddrs();
    List<Integer> targetXferPorts = plan.getTargetXferPorts();
    List<StorageType> targetStorageTypes = plan.getTargetStorageTypes();
    List<Long> blockIds = plan.getBlockIds();

    for (int planIndex = 0; planIndex < blockIds.size(); planIndex ++) {
      // build block
      Block block = sourceBlockMap.get(blockIds.get(planIndex));
      // build source
      DatanodeInfo sourceDatanode = sourceDatanodeMap.get(sourceUuids.get(planIndex));
      Dispatcher.DDatanode sourceDD = new Dispatcher.DDatanode(sourceDatanode, 10);
      StorageGroup source = new StorageGroup(sourceDD, sourceStorageTypes.get(planIndex));
      //build target
      DatanodeInfo targetDatanode = new DatanodeInfo(targetIpAddrs.get(planIndex),
          null, null,
          targetXferPorts.get(planIndex),
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null);
      Dispatcher.DDatanode targetDD = new Dispatcher.DDatanode(targetDatanode, 10);
      StorageGroup target = new StorageGroup(targetDD, targetStorageTypes.get(planIndex));
      // generate single move
      SingleMoveStatus status = new SingleMoveStatus();
      SingleMove singleMove = new SingleMove(block, source, target, status);
      allStatus.add(status);
      allMoves.add(singleMove);
    }
  }

  private void generateSourceMap() throws IOException {
    sourceBlockMap = new HashMap<>();
    sourceDatanodeMap = new HashMap<>();
    DFSClient dfsClient = nnc.getDistributedFileSystem().getClient();
    List<LocatedBlock> locatedBlocks = getLocatedBlocks(dfsClient, fileName);
    for (LocatedBlock locatedBlock : locatedBlocks) {
      sourceBlockMap.put(locatedBlock.getBlock().getBlockId(), locatedBlock.getBlock().getLocalBlock());
      for (DatanodeInfo datanodeInfo : locatedBlock.getLocations()) {
        sourceDatanodeMap.put(datanodeInfo.getDatanodeUuid(), datanodeInfo);
      }
    }
  }

  static List<LocatedBlock> getLocatedBlocks(DFSClient dfsClient, String fileName)
      throws IOException {
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileName);
    if (fileStatus == null) {
      throw new RuntimeException("File does not exist.");
    }
    long length = fileStatus.getLen();
    List<LocatedBlock> locatedBlocks = dfsClient.getLocatedBlocks(
        fileName, 0, length).getLocatedBlocks();
    return locatedBlocks;
  }

  /**
   * One single move represents the move action of one replication.
   */
  class SingleMove implements Runnable {
    private Block block;
    private StorageGroup target;
    private StorageGroup source;
    private SingleMoveStatus status;

    public SingleMove(Block block, StorageGroup source, StorageGroup target,
        SingleMoveStatus status) {
      this.block = block;
      this.target = target;
      this.source = source;
      this.status = status;
    }

    @Override
    public void run() {
      LOG.debug("Start moving " + this);

      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(
            NetUtils.createSocketAddr(target.getDatanodeInfo().getXferAddr()),
            HdfsServerConstants.READ_TIMEOUT);

        sock.setKeepAlive(true);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(), block);
        final KeyManager km = nnc.getKeyManager();
        Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb);
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
            unbufIn, km, accessToken, target.getDatanodeInfo());
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.IO_FILE_BUFFER_SIZE));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            HdfsConstants.IO_FILE_BUFFER_SIZE));

        sendRequest(out, eb, accessToken);
        receiveResponse(in);
        LOG.info("Successfully moved " + this);
        status.setSuccessful(true);
      } catch (IOException e) {
        LOG.warn("Failed to move " + this + ": " + e.getMessage());
        status.setSuccessful(false);
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
        status.setFinished(true);
      }
    }

    /** Send a block replace request to the output stream */
    private void sendRequest(DataOutputStream out, ExtendedBlock eb,
        Token<BlockTokenIdentifier> accessToken) throws IOException {
      new Sender(out).replaceBlock(eb, target.storageType, accessToken,
          source.getDatanodeInfo().getDatanodeUuid(), source.getDatanodeInfo());
    }

    /** Receive a block copy response from the input stream */
    private void receiveResponse(DataInputStream in) throws IOException {
      DataTransferProtos.BlockOpResponseProto response =
          DataTransferProtos.BlockOpResponseProto.parseFrom(vintPrefixed(in));
      while (response.getStatus() == DataTransferProtos.Status.IN_PROGRESS) {
        // read intermediate responses
        response = DataTransferProtos.BlockOpResponseProto.parseFrom(vintPrefixed(in));
      }
      String logInfo = "block move is failed";
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
    }
  }

  /**
   * A class for tracking the status of a single move.
   */
  static class SingleMoveStatus {
    private boolean finished;
    private boolean successful;

    public SingleMoveStatus() {
      finished = false;
      successful = false;
    }

    public void setFinished(boolean finished) {
      this.finished = finished;
    }

    public void setSuccessful(boolean successful) {
      this.successful = successful;
    }

    public boolean isFinished() {
      return finished;
    }

    public boolean isSuccessful() {
      return successful;
    }

    public static boolean allMoveFinished(List<SingleMoveStatus> allStatus) {
      for (SingleMoveStatus status : allStatus) {
        if (!status.isFinished()){
          return false;
        }
      }
      return true;
    }

    public static int failedMoves(List<SingleMoveStatus> allStatus) {
      int failedNum = 0;
      for (SingleMoveStatus status : allStatus) {
        if (!status.isSuccessful()) {
          failedNum += 1;
        }
      }
      return failedNum;
    }
  }
}
