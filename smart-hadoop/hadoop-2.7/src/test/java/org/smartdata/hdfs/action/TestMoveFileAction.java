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
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.MockActionStatusReporter;
import org.smartdata.hdfs.action.move.MoverStatus;
import org.smartdata.protocol.message.ActionFinished;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReporter;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for MoveFileAction.
 */
public class TestMoveFileAction extends ActionMiniCluster {
  @Test(timeout = 300000)
  public void testParallelMovers() throws Exception {
    final String file1 = "/testParallelMovers/file1";
    final String file2 = "/testParallelMovers/file2";
    Path dir = new Path("/testParallelMovers");
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out1 = dfs.create(new Path(file1));
    out1.writeChars("testParallelMovers1");
    out1.close();
    final FSDataOutputStream out2 = dfs.create(new Path(file2));
    out2.writeChars("testParallelMovers2");
    out2.close();

    // schedule move to ARCHIVE or SSD
    ArchiveFileAction action1 = new ArchiveFileAction();
    action1.setDfsClient(dfsClient);
    action1.setContext(smartContext);
    action1.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args1 = new HashMap();
    args1.put(ArchiveFileAction.FILE_PATH, file1);
    action1.init(args1);

    AllSsdFileAction action2 = new AllSsdFileAction();
    action2.setDfsClient(dfsClient);
    action2.setContext(smartContext);
    action2.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> args2 = new HashMap();
    args2.put(AllSsdFileAction.FILE_PATH, file2);
    action2.init(args2);

    action1.run();
    action2.run();
  }

  @Test(timeout = 300000)
  public void testMoverPercentage() throws Exception {
    final String file1 = "/testParallelMovers/file1";
    final String file2 = "/testParallelMovers/child/file2";
    String dir = "/testParallelMovers";
    dfs.mkdirs(new Path(dir));
    dfs.mkdirs(new Path("/testParallelMovers/child"));

    // write to DISK
    dfs.setStoragePolicy(new Path(dir), "HOT");
    final FSDataOutputStream out1 = dfs.create(new Path(file1), (short)5);
    final String string1 = "testParallelMovers1";
    out1.writeChars(string1);
    out1.close();
    final FSDataOutputStream out2 = dfs.create(new Path(file2));
    final String string2 = "testParallelMovers212345678901234567890";
    out2.writeChars(string2);
    out2.close();

    // schedule move to ALL_SSD
    long totalSize1 = string1.length()*2*5;
    long blockNum1 = 1*5;
    long totalSize2 = string2.length()*2*3;
    long blockNum2 = 2*3;
    scheduleMoverWithPercentage(dir, "ALL_SSD", totalSize1 + totalSize2,
        blockNum1 + blockNum2);

    // schedule move to ONE_SSD
    totalSize1 = string1.length()*2*4;
    blockNum1 = 1*4;
    totalSize2 = string2.length()*2*2;
    blockNum2 = 2*2;
    scheduleMoverWithPercentage(dir, "ONE_SSD", totalSize1 + totalSize2,
        blockNum1 + blockNum2);
  }

  private void scheduleMoverWithPercentage(String dir, String storageType,
      long totalSize, long totolBlocks) throws Exception {
    MoveFileAction moveFileAction = new MoveFileAction();
    moveFileAction.setDfsClient(dfsClient);
    moveFileAction.setContext(smartContext);
    moveFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap();
    args.put(MoveFileAction.FILE_PATH, dir);
    args.put(MoveFileAction.STORAGE_POLICY, storageType);
    moveFileAction.init(args);
    moveFileAction.run();

    MoverStatus moverStatus = moveFileAction.getStatus();
    System.out.println("Mover is finished.");
    Assert.assertEquals(1.0f, moverStatus.getPercentage(), 0.00001f);
    Assert.assertEquals(1.0f, moveFileAction.getProgress(), 0.00001f);
    Assert.assertEquals(totalSize, moverStatus.getTotalSize());
    Assert.assertEquals(totolBlocks, moverStatus.getTotalBlocks());
  }

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
  public void testMultiblockFile() throws Exception {
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
    action1.init(args1);
    action1.run();

    MoverStatus status = action1.getStatus();
    Assert.assertEquals(1.0f, status.getPercentage(), 0.0000001f);
  }
}
