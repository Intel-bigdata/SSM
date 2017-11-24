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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.hdfs.MiniClusterWithStoragesHarness;
import org.smartdata.model.action.FileMovePlan;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;

/**
 * Test for MoverExecutor.
 */
public class TestMoverExecutor extends MiniClusterWithStoragesHarness {
  private final String fileName = "/test/file";
  private final String fileDir = "/test";

  private void generateFile(String content) throws Exception {
    Path dir = new Path(fileDir);
    if (dfs.exists(dir)) {
      dfs.delete(dir, true);
    }
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out = dfs.create(new Path(fileName));
    out.writeChars(content);
    out.close();
  }

  @Test
  public void moveInSameNode() throws Exception {
    Configuration conf = smartContext.getConf();
    URI namenode = cluster.getURI();

    String blockContent = "This is a block with 50B.";
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 50; i ++) {
      stringBuilder.append(blockContent);
    }
    String content = stringBuilder.toString();
    generateFile(content);

    FileMovePlan plan = new FileMovePlan(namenode, fileName);

    // Schedule move in the same node
    for (LocatedBlock lb : getLocatedBlocks(dfsClient, fileName, plan)) {
      ExtendedBlock block = lb.getBlock();
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        StorageGroup source = new StorageGroup(datanodeInfo, StorageType.DISK.toString());
        StorageGroup target = new StorageGroup(datanodeInfo, StorageType.SSD.toString());
        addPlan(plan, source, target, block.getBlockId());
      }
    }

    // Do move executor
    MoverStatus status = new MoverStatus();
    MoverExecutor moverExecutor = new MoverExecutor(status, conf, 10, 3);
    int failedMoves = moverExecutor.executeMove(plan);
    Assert.assertEquals(0, failedMoves);
    cluster.triggerBlockReports();

    boolean success = true;
    for (int i = 0; i < 3; i++) {
      success = true;
      // Check storage after move
      for (LocatedBlock lb : getLocatedBlocks(dfsClient, fileName)) {
        for (DatanodeInfo datanodeInfo : lb.getLocations()) {
          StorageType realType = ((DatanodeInfoWithStorage) datanodeInfo).getStorageType();
          success = realType == StorageType.SSD && success;
        }
      }

      if (success) {
        break;
      }
      Thread.sleep(500);
    }
    if (!success) {
      Assert.fail("Not the expected storage type SSD.");
    }
  }

  @Test
  // TODO: seems the original replica is not deleted after move
  // DataXceiver.replaceBlock doing this job
  public void moveCrossNodes() throws Exception {
    Configuration conf = smartContext.getConf();
    URI namenode = cluster.getURI();

    generateFile("One-block file");

    FileMovePlan plan = new FileMovePlan(namenode, fileName);

    // Schedule move of one replica to another node
    NameNodeConnector nnc = new NameNodeConnector(namenode, conf);
    HashSet<DatanodeInfo> fileNodes = new HashSet<>();
    ExtendedBlock block = null;
    for (LocatedBlock lb : getLocatedBlocks(dfsClient, fileName, plan)) {
      block = lb.getBlock();
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        fileNodes.add(datanodeInfo);
      }
    }
    final DatanodeStorageReport[] reports = nnc.getLiveDatanodeStorageReport();
    for (DatanodeStorageReport report : reports) {
      DatanodeInfo targetDatanode = report.getDatanodeInfo();
      if (!fileNodes.contains(targetDatanode)) {
        StorageGroup source = new StorageGroup(fileNodes.iterator().next(), StorageType.DISK.toString());
        StorageGroup target = new StorageGroup(targetDatanode, StorageType.SSD.toString());
        addPlan(plan, source, target, block.getBlockId());
        break;
      }
    }

    // Do mover executor
    MoverStatus status = new MoverStatus();
    MoverExecutor moverExecutor = new MoverExecutor(status, conf, 10, 500);
    int failedMoves = moverExecutor.executeMove(plan);
    Assert.assertEquals(0, failedMoves);

    // Check storage after move
    //Thread.sleep(100000);
    int ssdNum = 0;
    int hddNum = 0;
    for (LocatedBlock lb : getLocatedBlocks(dfsClient, fileName)) {
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        Assert.assertTrue(datanodeInfo instanceof DatanodeInfoWithStorage);
        StorageType storageType = ((DatanodeInfoWithStorage)datanodeInfo).getStorageType();
        if (storageType.equals(StorageType.SSD)) {
          ssdNum ++;
        } else if (storageType.equals(StorageType.DISK)) {
          hddNum ++;
        }
      }
    }
//    Assert.assertEquals(1, ssdNum);
//    Assert.assertEquals(2, hddNum);
  }

  private void addPlan(FileMovePlan plan, StorageGroup source, StorageGroup target, long blockId) {
    DatanodeInfo sourceDatanode = source.getDatanodeInfo();
    DatanodeInfo targetDatanode = target.getDatanodeInfo();
    plan.addPlan(blockId, sourceDatanode.getDatanodeUuid(), source.getStorageType(),
        targetDatanode.getIpAddr(), targetDatanode.getXferPort(), target.getStorageType());
  }

  private List<LocatedBlock> getLocatedBlocks(DFSClient dfsClient, String fileName, FileMovePlan plan)
      throws IOException {
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileName);
    if (fileStatus == null) {
      throw new IOException("File does not exist.");
    }
    long length = fileStatus.getLen();
    if (plan != null) {
      plan.setFileLength(length);
    }
    return dfsClient.getLocatedBlocks(fileName, 0, length).getLocatedBlocks();
  }

  private List<LocatedBlock> getLocatedBlocks(DFSClient dfsClient, String fileName)
      throws IOException {
    return getLocatedBlocks(dfsClient, fileName, null);
  }
}
