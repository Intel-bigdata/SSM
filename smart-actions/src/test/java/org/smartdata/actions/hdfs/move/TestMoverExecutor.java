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
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.hdfs.ActionMiniCluster;

import java.net.URI;

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

    String content = "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B.";
    generateFile(content);

    SchedulePlan plan = new SchedulePlan(namenode, fileName);

    // Schedule move in the same node
    for (LocatedBlock lb : MoverExecutor.getLocatedBlocks(dfsClient, fileName)) {
      ExtendedBlock block = lb.getBlock();
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        Dispatcher.DDatanode datanode = new Dispatcher.DDatanode(datanodeInfo, 100);
        StorageGroup source = new StorageGroup(datanode, StorageType.DISK);
        StorageGroup target = new StorageGroup(datanode, StorageType.SSD);
        plan.addPlan(source, target, block);
      }
    }

    // Do move executor
    MoverExecutor moverExecutor = new MoverExecutor(conf, 500);
    int failedMoves = moverExecutor.executeMove(plan);

    // Check storage after move
    for (LocatedBlock lb : MoverExecutor.getLocatedBlocks(dfsClient, fileName)) {
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        Assert.assertTrue(datanodeInfo instanceof DatanodeInfoWithStorage);
        Assert.assertEquals(StorageType.SSD, ((DatanodeInfoWithStorage)datanodeInfo).getStorageType());
      }
    }
  }

  @Test
  public void moveCrossNodes() throws Exception {
    Configuration conf = smartContext.getConf();
    URI namenode = cluster.getURI();

    //generateFile();

    SchedulePlan plan = new SchedulePlan(namenode, fileName);

    // Move one replica to another node

  }
}
