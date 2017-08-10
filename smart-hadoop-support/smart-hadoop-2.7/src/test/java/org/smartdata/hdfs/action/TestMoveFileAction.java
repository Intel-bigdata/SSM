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
package org.smartdata.hdfs.action;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;
import org.smartdata.hdfs.action.move.MoverExecutor;
import org.smartdata.hdfs.action.move.MoverStatus;
import org.smartdata.hdfs.action.move.StorageGroup;
import org.smartdata.protocol.message.ActionFinished;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReporter;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for MoveFileAction.
 */
public class TestMoveFileAction extends MiniClusterHarness {

  @Test(timeout = 300000)
  public void testParallelMove() throws Exception {
    String dir = "/test";
    String file1 = "/test/file1";
    String file2 = "/test/file2";
    dfs.mkdirs(new Path(dir));

    //write to DISK
    dfs.setStoragePolicy(new Path(dir), "HOT");
    FSDataOutputStream out1 = dfs.create(new Path(file1));
    final String str1 = "testtesttest1";
    out1.writeChars(str1);
    out1.close();
    FSDataOutputStream out2 = dfs.create(new Path(file2));
    final String str2 = "testtesttest2";
    out2.writeChars(str2);
    out2.close();

    //move to SSD
    MoveFileAction moveFileAction1 = new MoveFileAction();
    moveFileAction1.setDfsClient(dfsClient);
    moveFileAction1.setContext(smartContext);
    moveFileAction1.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> args1 = new HashMap();
    args1.put(MoveFileAction.FILE_PATH, dir);
    String storageType1 = "ONE_SSD";
    args1.put(MoveFileAction.STORAGE_POLICY, storageType1);
    SchedulePlan plan = createPlan(file1, storageType1);
    args1.put(MoveFileAction.MOVE_PLAN, plan.toString());

    MoveFileAction moveFileAction2 = new MoveFileAction();
    moveFileAction2.setDfsClient(dfsClient);
    moveFileAction2.setContext(smartContext);
    moveFileAction2.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> args2 = new HashMap();
    args2.put(MoveFileAction.FILE_PATH, dir);
    String storageType2 = "ONE_SSD";
    args2.put(MoveFileAction.STORAGE_POLICY, storageType2);
    SchedulePlan plan2 = createPlan(file1, storageType2);
    args2.put(MoveFileAction.MOVE_PLAN, plan.toString());

    moveFileAction1.init(args1);
    moveFileAction2.init(args2);

    moveFileAction1.run();
    moveFileAction2.run();
  }

  @Test(timeout = 300000)
  public void testMove() throws Exception{
    String dir = "/test";
    String file = "/test/file";
    dfs.mkdirs(new Path(dir));

    //write to DISK
    dfs.setStoragePolicy(new Path(dir), "HOT");
    FSDataOutputStream out = dfs.create(new Path(file));
    final String str = "testtesttest";
    out.writeChars(str);

    //move to SSD
    MoveFileAction moveFileAction = new MoveFileAction();
    moveFileAction.setDfsClient(dfsClient);
    moveFileAction.setContext(smartContext);
    moveFileAction.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> args = new HashMap();
    args.put(MoveFileAction.FILE_PATH, dir);
    String storageType = "ONE_SSD";
    args.put(MoveFileAction.STORAGE_POLICY, storageType);
    SchedulePlan plan = createPlan(file, storageType);
    args.put(MoveFileAction.MOVE_PLAN, plan.toString());

    moveFileAction.init(args);
    moveFileAction.run();
  }



//  @Test(timeout = 300000)
//  public void testMoverPercentage() throws Exception {
//    final String file1 = "/testParallelMovers/file1";
//    final String file2 = "/testParallelMovers/child/file2";
//    String dir = "/testParallelMovers";
//    dfs.mkdirs(new Path(dir));
//    dfs.mkdirs(new Path("/testParallelMovers/child"));
//
//    // write to DISK
//    dfs.setStoragePolicy(new Path(dir), "HOT");
//    final FSDataOutputStream out1 = dfs.create(new Path(file1), (short)5);
//    final String string1 = "testParallelMovers1";
//    out1.writeChars(string1);
//    out1.close();
//    final FSDataOutputStream out2 = dfs.create(new Path(file2));
//    final String string2 = "testParallelMovers212345678901234567890";
//    out2.writeChars(string2);
//    out2.close();
//
//    // schedule move to ALL_SSD
//    long totalSize1 = string1.length()*2*5;
//    long blockNum1 = 1*5;
//    long totalSize2 = string2.length()*2*3;
//    long blockNum2 = 2*3;
//    scheduleMoverWithPercentage(file1, "ALL_SSD", totalSize1,
//        blockNum1);
//
//    // schedule move to ONE_SSD
//    totalSize1 = string1.length()*2*4;
//    blockNum1 = 1*4;
//    totalSize2 = string2.length()*2*2;
//    blockNum2 = 2*2;
//    scheduleMoverWithPercentage(dir, "ONE_SSD", totalSize1 + totalSize2,
//        blockNum1 + blockNum2);
//  }
//
//  private void scheduleMoverWithPercentage(String dir, String storageType,
//                                           long totalSize, long totolBlocks) throws Exception {
//    MoveFileAction moveFileAction = new MoveFileAction();
//    moveFileAction.setDfsClient(dfsClient);
//    moveFileAction.setContext(smartContext);
//    moveFileAction.setStatusReporter(new MockActionStatusReporter());
//    Map<String, String> args = new HashMap();
//    args.put(MoveFileAction.FILE_PATH, dir);
//    args.put(MoveFileAction.STORAGE_POLICY, storageType);
//    SchedulePlan plan = createPlan(dir, storageType);
//    args.put(MoveFileAction.MOVE_PLAN, plan.toString());
//
//    moveFileAction.init(args);
//    moveFileAction.run();
//
//    MoverStatus moverStatus = moveFileAction.getStatus();
//    System.out.println("Mover is finished.");
//    Assert.assertEquals(1.0f, moverStatus.getPercentage(), 0.00001f);
//    Assert.assertEquals(1.0f, moveFileAction.getProgress(), 0.00001f);
//    Assert.assertEquals(totalSize, moverStatus.getTotalSize());
//    Assert.assertEquals(totolBlocks, moverStatus.getTotalBlocks());
//  }

  @Test(timeout = 300000)
  public void testMoveNonexitedFile() throws Exception {
    String dir = "/testParallelMovers";

    // schedule move to ALL_SSD
    MoveFileAction moveFileAction = new MoveFileAction();
    moveFileAction.setDfsClient(dfsClient);
    moveFileAction.setContext(smartContext);
    moveFileAction.setStatusReporter(new StatusReporter() {
      @Override
      public void report(StatusMessage status) {
        if (status instanceof ActionFinished) {
          ActionFinished finished = (ActionFinished) status;
          Assert.assertNotNull(finished.getThrowable());
        }
      }
    });

    Map<String, String> args = new HashMap();
    args.put(MoveFileAction.FILE_PATH, dir);
    args.put(MoveFileAction.STORAGE_POLICY, "ALL_SSD");
    moveFileAction.init(args);
    moveFileAction.run();
  }

  @Test
  public void testMoveMultiblockFile() throws Exception {
    final String file1 = "/testParallelMovers/file1";
    Path dir = new Path("/testParallelMovers");
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out1 = dfs.create(new Path(file1));
    out1.writeChars("This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B." +
        "This is a block with 50B.");
    out1.close();

    // schedule move to ARCHIVE or SSD
    ArchiveFileAction action1 = new ArchiveFileAction();
    action1.setDfsClient(dfsClient);
    action1.setContext(smartContext);
    action1.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args1 = new HashMap();
    args1.put(ArchiveFileAction.FILE_PATH, file1);
    args1.put(MoveFileAction.MOVE_PLAN, null);
    SchedulePlan plan = createPlan(file1, "SSD");
    args1.put(MoveFileAction.MOVE_PLAN, plan.toString());
    action1.init(args1);
    action1.run();
  }

  private SchedulePlan createPlan(String dir, String storageType) throws Exception {

    URI namenode = cluster.getURI();

    SchedulePlan plan = new SchedulePlan(namenode, dir);

//     Schedule move in the same node
    for (LocatedBlock lb : MoverExecutor.getLocatedBlocks(dfsClient, dir)) {
      ExtendedBlock block = lb.getBlock();
      for (DatanodeInfo datanodeInfo : lb.getLocations()) {
        StorageGroup source = new StorageGroup(datanodeInfo, StorageType.DISK.toString());
        StorageGroup target = new StorageGroup(datanodeInfo, storageType);
        addPlan(plan, source, target, block.getBlockId());
      }
    }

    return plan;
  }

  private void addPlan(SchedulePlan plan, StorageGroup source, StorageGroup target, long blockId) {
    DatanodeInfo sourceDatanode = source.getDatanodeInfo();
    DatanodeInfo targetDatanode = target.getDatanodeInfo();
    plan.addPlan(blockId, sourceDatanode.getDatanodeUuid(), source.getStorageType(),
        targetDatanode.getIpAddr(), targetDatanode.getXferPort(), target.getStorageType());
  }
}
