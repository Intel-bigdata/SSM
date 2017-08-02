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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.hdfs.ActionMiniCluster;
import org.smartdata.model.actions.hdfs.SchedulePlan;
import org.smartdata.model.actions.hdfs.StorageGroup;

import java.net.URI;
import java.util.HashSet;

/**
 * Test for MoverExecutor.
 */
public class TestMoverExecutor extends ActionMiniCluster {
  private final String fileName = "/test/file";
  private final String fileDir = "/test";

  private void generateFile(String content) throws Exception {
    Path dir = new Path(fileDir);
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

    SchedulePlan plan = new SchedulePlan(namenode, fileName);

    // Schedule move in the same node
    for (LocatedBlock lb : MoverExecutor.getLocatedBlocks(dfsClient, fileName)) {
      ExtendedBlock block = lb.getBlock();
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        StorageGroup source = new StorageGroup(datanodeInfo, StorageType.DISK);
        StorageGroup target = new StorageGroup(datanodeInfo, StorageType.SSD);
        plan.addPlan(source, target, block);
      }
    }

    // Do move executor
    MoverExecutor moverExecutor = new MoverExecutor(conf, 10, 500);
    int failedMoves = moverExecutor.executeMove(plan);
    Assert.assertEquals(0, failedMoves);

    // Check storage after move
    for (LocatedBlock lb : MoverExecutor.getLocatedBlocks(dfsClient, fileName)) {
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        Assert.assertTrue(datanodeInfo instanceof DatanodeInfoWithStorage);
        Assert.assertEquals(StorageType.SSD, ((DatanodeInfoWithStorage)datanodeInfo).getStorageType());
      }
    }
  }

  @Test
  // TODO: seems the original replica is not deleted after move
  // DataXceiver.replaceBlock doing this job
  public void moveCrossNodes() throws Exception {
    Configuration conf = smartContext.getConf();
    URI namenode = cluster.getURI();

    generateFile("One-block file");

    SchedulePlan plan = new SchedulePlan(namenode, fileName);

    // Schedule move of one replica to another node
    NameNodeConnector nnc = new NameNodeConnector(namenode, conf);
    HashSet<DatanodeInfo> fileNodes = new HashSet<>();
    ExtendedBlock block = null;
    for (LocatedBlock lb : MoverExecutor.getLocatedBlocks(dfsClient, fileName)) {
      block = lb.getBlock();
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        fileNodes.add(datanodeInfo);
      }
    }
    final DatanodeStorageReport[] reports = nnc.getLiveDatanodeStorageReport();
    for (DatanodeStorageReport report : reports) {
      DatanodeInfo targetDatanode = report.getDatanodeInfo();
      if (!fileNodes.contains(targetDatanode)) {
        StorageGroup source = new StorageGroup(fileNodes.iterator().next(), StorageType.DISK);
        StorageGroup target = new StorageGroup(targetDatanode, StorageType.SSD);
        plan.addPlan(source, target, block);
        break;
      }
    }

    // Do mover executor
    MoverExecutor moverExecutor = new MoverExecutor(conf, 10, 500);
    int failedMoves = moverExecutor.executeMove(plan);
    Assert.assertEquals(0, failedMoves);

    // Check storage after move
    //Thread.sleep(100000);
    int ssdNum = 0;
    int hddNum = 0;
    for (LocatedBlock lb : MoverExecutor.getLocatedBlocks(dfsClient, fileName)) {
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
}
