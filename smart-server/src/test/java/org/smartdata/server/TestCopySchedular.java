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
package org.smartdata.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.server.engine.CopyScheduler;
import org.smartdata.server.engine.CopyTargetTask;

import java.io.IOException;
import java.util.List;

public class TestCopySchedular extends TestEmptyMiniSmartCluster {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFileDiffParsing() throws Exception {
    waitTillSSMExitSafeMode();
    FileDiff fileDiff = new FileDiff();
    fileDiff.setDiffType(FileDiffType.APPEND);
    // Create and write
    fileDiff.setParameters("-file /root/test");
    String cmd =
        CopyScheduler.cmdParsing(fileDiff, "/root/", "/localhost:3306/backup/");
    Assert.assertTrue(
        cmd.equals("copy -file /root/test -dest /localhost:3306/backup/test"));
    fileDiff.setParameters("-file /root/test -length 1024");
    cmd =
        CopyScheduler.cmdParsing(fileDiff, "/root/", "/localhost:3306/backup/");
    Assert.assertTrue(cmd.equals(
        "copy -file /root/test -dest /localhost:3306/backup/test -length 1024"));
    // Rename
    fileDiff.setDiffType(FileDiffType.RENAME);
    fileDiff.setParameters("-file /root/test -dest /root/test2 -length 1024");
    cmd =
        CopyScheduler.cmdParsing(fileDiff, "/root/", "/localhost:3306/backup/");
    Assert.assertTrue(
        cmd.equals(
            "rename -file /localhost:3306/backup/test -dest /localhost:3306/backup/test2 -length 1024"));
  }

  @Test
  public void testForceSync() throws Exception {
    waitTillSSMExitSafeMode();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    DFSClient client =
        new DFSClient(cluster.getURI(), ssm.getContext().getConf());
    CopyScheduler copyScheduler = new CopyScheduler(ssm.getContext(),
        ssm.getCmdletManager(), client, srcPath, destPath);
    // Write to src
    for (int i = 0; i < 5; i++) {
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
    }
    // Clear file_diffs
    Thread.sleep(1500);
    ssm.getMetaStore().deleteAllFileDiff();

    // init forceSync
    copyScheduler.forceSync(srcPath, destPath);
    copyScheduler.start();
    Thread.sleep(2200);
    while (true) {
      Thread.sleep(1500);
      int current = ssm.getCmdletManager().getCmdletsSizeInCache();
      System.out.printf("Current running cmdlet number: %d\n", current);
      if (current == 0) {
        break;
      }
    }
    for (int i = 0; i < 5; i++) {
      // Write 10 files
      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
      System.out.printf("File %d is force copied.\n", i);
    }
    copyScheduler.stop();
  }


  @Test
  public void testDiffApplied() throws Exception {
    waitTillSSMExitSafeMode();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    DFSClient client =
        new DFSClient(cluster.getURI(), ssm.getContext().getConf());
    CopyScheduler copyScheduler = new CopyScheduler(ssm.getContext(),
        ssm.getCmdletManager(), client, srcPath, destPath);
    copyScheduler.start();
    for (int i = 0; i < 5; i++) {
      // Write 10 files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
    }
    Thread.sleep(2200);
    while (true) {
      Thread.sleep(1500);
      int current = ssm.getCmdletManager().getCmdletsSizeInCache();
      System.out.printf("Current running cmdlet number: %d\n", current);
      if (current == 0) {
        break;
      }
    }
    for (int i = 0; i < 5; i++) {
      // Write 10 files
      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
      System.out.printf("File %d is copied.\n", i);
    }
    copyScheduler.stop();
  }


  @Test
  public void testCopyScheduler() throws IOException {
    DistributedFileSystem dfs = cluster.getFileSystem();

    final String srcPath = "/testCopy";
    final String file1 = "file1";
    final String destPath = "/backup";

    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    for (int i = 0; i < 50; i++) {
      out1.writeByte(1);
    }
    for (int i = 0; i < 50; i++) {
      out1.writeByte(2);
    }
    for (int i = 0; i < 50; i++) {
      out1.writeByte(3);
    }
    out1.close();

    // System.out.println(dfs.getFileStatus(new Path(srcPath + "/" + file1)).getLen());
    List<CopyTargetTask> copyTargetTaskList =
        CopyScheduler.splitCopyFile(srcPath + "/" + file1,
            destPath + "/" + file1, 1,
            FileSystem.get(dfs.getUri(), new Configuration()));

    System.out.println(copyTargetTaskList);
    // block_size = 100, then only two files
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(copyTargetTaskList.get(i).getDest()
          .equals("/backup/file1_temp_chunkCount" + (i + 1)));
    }
  }
}
