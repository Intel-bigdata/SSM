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

/*import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;
import org.smartdata.model.RuleState;
import org.smartdata.server.engine.CmdletManager;


import java.util.List;

public class TestCopyScheduler extends MiniSmartClusterHarness {

  @Test(timeout = 45000)
  public void appendMerge() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    CmdletManager cmdletManager = ssm.getCmdletManager();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    BackUpInfo backUpInfo = new BackUpInfo(1L, srcPath, destPath, 100);
    metaStore.insertBackUpInfo(backUpInfo);
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
      for (int j = 0; j < 10; j++) {
        DFSTestUtil.appendFile(dfs, new Path(srcPath + i), 1024);
      }
    }
    do {
      Thread.sleep(1500);
    } while (metaStore.getPendingDiff().size() >= 30);
    List<FileDiff> fileDiffs = metaStore.getFileDiffs(FileDiffState.PENDING);
    Assert.assertTrue(fileDiffs.size() < 30);
  }

  @Test(timeout = 45000)
  public void deleteMerge() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    CmdletManager cmdletManager = ssm.getCmdletManager();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    BackUpInfo backUpInfo = new BackUpInfo(1L, srcPath, destPath, 100);
    metaStore.insertBackUpInfo(backUpInfo);
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
      do {
        Thread.sleep(500);
      } while (!dfs.isFileClosed(new Path(srcPath + i)));
      dfs.delete(new Path(srcPath + i), false);
    }
    Thread.sleep(1200);
    List<FileDiff> fileDiffs;
    fileDiffs = metaStore.getFileDiffs(FileDiffState.PENDING);
    while (fileDiffs.size() != 0) {
      Thread.sleep(1000);
      for (FileDiff fileDiff : fileDiffs) {
        System.out.println(fileDiff.toString());
      }
      fileDiffs = metaStore.getFileDiffs(FileDiffState.PENDING);
    }
    // File is not created, so clear all fileDiff
  }

  @Test(timeout = 45000)
  public void renameMerge() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    BackUpInfo backUpInfo = new BackUpInfo(1L, srcPath, destPath, 100);
    metaStore.insertBackUpInfo(backUpInfo);
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
      dfs.rename(new Path(srcPath + i), new Path(srcPath + i + 10));
      // Rename target ends with 10
      DFSTestUtil.appendFile(dfs, new Path(srcPath + i + 10), 1024);
    }
    do {
      Thread.sleep(1500);
    } while (metaStore.getPendingDiff().size() < 9);
    Thread.sleep(1000);
    List<FileDiff> fileDiffs = metaStore.getFileDiffs(FileDiffState.PENDING);
    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getDiffType() == FileDiffType.APPEND) {
        Assert.assertTrue(fileDiff.getSrc().endsWith("10"));
      }
    }
  }

  @Test(timeout = 45000)
  public void failRetry() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    long ruleId =
        admin.submitRule(
            "file: every 1s | path matches \"/src/*\"| sync -dest /dest/", RuleState.ACTIVE);
    FileDiff fileDiff = new FileDiff(FileDiffType.RENAME, FileDiffState.PENDING);
    fileDiff.setSrc("/src/1");
    fileDiff.getParameters().put("-dest", "/src/2");
    metaStore.insertFileDiff(fileDiff);
    Thread.sleep(1200);
    while (metaStore.getPendingDiff().size() != 0) {
      Thread.sleep(1000);
    }
    Thread.sleep(2000);
    fileDiff = metaStore.getFileDiffsByFileName("/src/1").get(0);
    Assert.assertTrue(fileDiff.getState() == FileDiffState.FAILED);

  }

  @Test
  public void testForceSync() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    CmdletManager cmdletManager = ssm.getCmdletManager();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
    }

    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(destPath + i + 5), 1024, (short) 1, 1);
    }

    // Clear file diffs
    metaStore.deleteAllFileDiff();
    // Submit rules and trigger forceSync
    long ruleId =
        admin.submitRule(
            "file: every 2s | path matches \"/src/*\"| sync -dest /dest/", RuleState.ACTIVE);
    Thread.sleep(1000);
    Assert.assertTrue(metaStore.getFileDiffs(FileDiffState.PENDING).size() > 0);
  }

  @Test(timeout = 40000)
  public void batchSync() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    FileInfo fileInfo;
    long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      fileInfo = new FileInfo(srcPath + i, i,  1024, false, (short)3,
          1024, now, now, (short) 1, null, null, (byte)3);
      metaStore.insertFile(fileInfo);
      Thread.sleep(100);
    }
    long ruleId =
        admin.submitRule(
            "file: every 2s | path matches \"/src/*\"| sync -dest /dest/", RuleState.ACTIVE);
    Thread.sleep(2200);
    do {
      Thread.sleep(1000);
    } while (metaStore.getPendingDiff().size() != 100);
  }

  @Test(timeout = 60000)
  public void testDelete() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    long ruleId =
        admin.submitRule(
            "file: every 2s | path matches \"/src/*\"| sync -dest /dest/", RuleState.ACTIVE);
    FileDiff fileDiff = new FileDiff(FileDiffType.DELETE, FileDiffState.PENDING);
    fileDiff.setSrc("/src/1");
    metaStore.insertFileDiff(fileDiff);
    Thread.sleep(1200);
    do {
      Thread.sleep(1000);
    } while (admin.getRuleInfo(ruleId).getNumCmdsGen() == 0);
    Assert.assertTrue(cmdletManager.listNewCreatedActions("sync", 0).size() > 0);
  }

  @Test(timeout = 60000)
  public void testRename() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    long ruleId =
        admin.submitRule(
            "file: every 2s | path matches \"/src/*\"| sync -dest /dest/", RuleState.ACTIVE);
    FileDiff fileDiff = new FileDiff(FileDiffType.RENAME, FileDiffState.PENDING);
    fileDiff.setSrc("/src/1");
    fileDiff.getParameters().put("-dest", "/src/2");
    metaStore.insertFileDiff(fileDiff);
    Thread.sleep(1200);
    do {
      Thread.sleep(1000);
    } while (admin.getRuleInfo(ruleId).getNumCmdsGen() == 0);
    Assert.assertTrue(cmdletManager.listNewCreatedActions("sync", 0).size() > 0);
  }

  @Test
  public void testMeta() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    long ruleId =
        admin.submitRule(
            "file: every 2s | path matches \"/src/*\"| sync -dest /dest/", RuleState.ACTIVE);
    Thread.sleep(4200);
    // Write to src
    DFSTestUtil.createFile(dfs, new Path(srcPath + 1), 1024, (short) 1, 1);
    Thread.sleep(1000);
    FileDiff fileDiff = new FileDiff(FileDiffType.METADATA, FileDiffState.PENDING);
    fileDiff.setSrc("/src/1");
    fileDiff.getParameters().put("-permission", "777");
    metaStore.insertFileDiff(fileDiff);
    do {
      Thread.sleep(1000);
    } while (admin.getRuleInfo(ruleId).getNumCmdsGen() == 0);
    while (metaStore.getPendingDiff().size() != 0) {
      Thread.sleep(500);
    }
    Thread.sleep(1000);
    FileStatus fileStatus = dfs.getFileStatus(new Path(destPath + 1));
    Assert.assertTrue(fileStatus.getPermission().toString().equals("rwxrwxrwx"));
  }

  @Test(timeout = 40000)
  public void testCache() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    CmdletManager cmdletManager = ssm.getCmdletManager();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    // Submit sync rule
    long ruleId =
        admin.submitRule(
            "file: every 2s | path matches \"/src/*\"| sync -dest " + destPath,
             RuleState.ACTIVE);
    Thread.sleep(2000);
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
    }
    do {
      Thread.sleep(1000);
    } while (admin.getRuleInfo(ruleId).getNumCmdsGen() <= 2);
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
    Assert.assertTrue(actionInfos.size() >= 3);
    Thread.sleep(20000);
  }

  @Test(timeout = 40000)
  public void testWithSyncRule() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    CmdletManager cmdletManager = ssm.getCmdletManager();
    // metaStore.deleteAllFileDiff();
    // metaStore.deleteAllFileInfo();
    // metaStore.deleteAllCmdlets();
    // metaStore.deleteAllActions();
    // metaStore.deleteAllRules();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    // Submit sync rule
    long ruleId =
        admin.submitRule(
            "file: every 2s | path matches \"/src/*\"| sync -dest " + destPath,
             RuleState.ACTIVE);
    Thread.sleep(2000);
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
    }
    do {
      Thread.sleep(1000);
    } while (admin.getRuleInfo(ruleId).getNumCmdsGen() <= 2);
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
    Assert.assertTrue(actionInfos.size() >= 3);
    do {
      Thread.sleep(800);
    } while (metaStore.getPendingDiff().size() != 0);
    for (int i = 0; i < 3; i++) {
      // Check 3 files
      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
      System.out.printf("File %d is copied.\n", i);
    }
  }

  @Test(timeout = 60000)
  public void testCopy() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    // metaStore.deleteAllFileDiff();
    // metaStore.deleteAllFileInfo();
    // metaStore.deleteAllCmdlets();
    // metaStore.deleteAllActions();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    BackUpInfo backUpInfo = new BackUpInfo(1L, srcPath, destPath, 100);
    metaStore.insertBackUpInfo(backUpInfo);
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
    }
    Thread.sleep(1000);
    CmdletManager cmdletManager = ssm.getCmdletManager();
    // Submit sync action
    for (int i = 0; i < 3; i++) {
      // Create test files
      cmdletManager.submitCmdlet(
          "sync -file /src/" + i + " -src " + srcPath + " -dest " + destPath);
    }
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
    Assert.assertTrue(actionInfos.size() >= 3);
    do {
      Thread.sleep(1000);

    } while (cmdletManager.getActionsSizeInCache() + cmdletManager.getCmdletsSizeInCache() > 0);
    for (int i = 0; i < 3; i++) {
      // Write 10 files
      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
      System.out.printf("File %d is copied.\n", i);
    }
  }

  @Test(timeout = 40000)
  public void testCopyDelete() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    // metaStore.deleteAllFileDiff();
    // metaStore.deleteAllFileInfo();
    // metaStore.deleteAllCmdlets();
    // metaStore.deleteAllActions();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    BackUpInfo backUpInfo = new BackUpInfo(1L, srcPath, destPath, 100);
    metaStore.insertBackUpInfo(backUpInfo);
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
      dfs.delete(new Path(srcPath + i), false);
    }

    Thread.sleep(2000);
    CmdletManager cmdletManager = ssm.getCmdletManager();
    // Submit sync action
    for (int i = 0; i < 3; i++) {
      // Create test files
      cmdletManager.submitCmdlet(
          "sync -file /src/" + i + " -src " + srcPath + " -dest " + destPath);
    }
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
    Assert.assertTrue(actionInfos.size() >= 3);
    Thread.sleep(3000);
    for (int i = 0; i < 3; i++) {
      // Write 10 files
      Assert.assertFalse(dfs.exists(new Path(destPath + i)));
      System.out.printf("File %d is copied.\n", i);
    }
  }
}*/
