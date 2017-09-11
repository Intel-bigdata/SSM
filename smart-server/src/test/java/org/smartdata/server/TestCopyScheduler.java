///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.smartdata.server;
//
//import org.apache.hadoop.hdfs.DFSTestUtil;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.hadoop.fs.Path;
//import org.junit.Assert;
//import org.junit.Test;
//import org.smartdata.admin.SmartAdmin;
//import org.smartdata.metastore.MetaStore;
//import org.smartdata.model.ActionInfo;
//import org.smartdata.model.FileDiff;
//import org.smartdata.model.FileDiffState;
//import org.smartdata.model.FileDiffType;
//import org.smartdata.model.RuleState;
//import org.smartdata.server.engine.CmdletManager;
//
//import java.util.List;
//
//public class TestCopyScheduler extends MiniSmartClusterHarness {
//
//   @Test
//   public void testDelete() throws Exception {
//     waitTillSSMExitSafeMode();
//     FileDiff fileDiff =
//         new FileDiff(FileDiffType.DELETE, FileDiffState.RUNNING);
//     fileDiff.setSrc("/src/1");
//     MetaStore metaStore = ssm.getMetaStore();
//     CmdletManager cmdletManager = ssm.getCmdletManager();
//     SmartAdmin admin = new SmartAdmin(smartContext.getConf());
//     metaStore.insertFileDiff(fileDiff);
//     long ruleId = admin.submitRule(
//         "file: every 1s | path matches \"/src/*\"| sync -dest /dest/",
//         RuleState.ACTIVE);
//     do {
//       Thread.sleep(1000);
//     } while (admin.getRuleInfo(ruleId).getNumCmdsGen() == 0);
//     Assert
//         .assertTrue(cmdletManager.listNewCreatedActions("sync", 0).size() > 0);
//   }
//
//  @Test (timeout = 40000)
//  public void testWithSyncRule() throws Exception {
//    waitTillSSMExitSafeMode();
//    MetaStore metaStore = ssm.getMetaStore();
//    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
//    CmdletManager cmdletManager = ssm.getCmdletManager();
//    // metaStore.deleteAllFileDiff();
//    // metaStore.deleteAllFileInfo();
//    // metaStore.deleteAllCmdlets();
//    // metaStore.deleteAllActions();
//    // metaStore.deleteAllRules();
//    DistributedFileSystem dfs = cluster.getFileSystem();
//    final String srcPath = "/src/";
//    final String destPath = "/dest/";
//    dfs.mkdirs(new Path(srcPath));
//    dfs.mkdirs(new Path(destPath));
//    // Write to src
//    for (int i = 0; i < 3; i++) {
//      // Create test files
//      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
//    }
//    Thread.sleep(1000);
//    // Submit sync rule
//    long ruleId = admin.submitRule(
//        "file: every 1s | path matches \"/src/*\"| sync -dest " + destPath,
//        RuleState.ACTIVE);
//    do {
//      Thread.sleep(1000);
//    } while(admin.getRuleInfo(ruleId).getNumCmdsGen() <= 2);
//    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
//    // Assert.assertTrue(actionInfos.size() == 3);
//    for (int i = 0; i < 3; i++) {
//      // Write 10 files
//      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
//      System.out.printf("File %d is copied.\n", i);
//    }
//  }
//
//
//  @Test (timeout = 60000)
//  public void testCopy() throws Exception {
//    waitTillSSMExitSafeMode();
//    MetaStore metaStore = ssm.getMetaStore();
//    // metaStore.deleteAllFileDiff();
//    // metaStore.deleteAllFileInfo();
//    // metaStore.deleteAllCmdlets();
//    // metaStore.deleteAllActions();
//    DistributedFileSystem dfs = cluster.getFileSystem();
//    final String srcPath = "/src/";
//    final String destPath = "/dest/";
//    dfs.mkdirs(new Path(srcPath));
//    dfs.mkdirs(new Path(destPath));
//    // Write to src
//    for (int i = 0; i < 3; i++) {
//      // Create test files
//      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
//    }
//    Thread.sleep(1000);
//    CmdletManager cmdletManager = ssm.getCmdletManager();
//    // Submit sync action
//    for (int i = 0; i < 3; i++) {
//      // Create test files
//      cmdletManager.submitCmdlet("sync -file /src/" + i + " -src " + srcPath + " -dest " + destPath);
//    }
//    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
//    Assert.assertTrue(actionInfos.size() >= 3);
//    do {
//      Thread.sleep(1000);
//
//    } while(cmdletManager.getActionsSizeInCache() + cmdletManager.getCmdletsSizeInCache() > 0);
//    for (int i = 0; i < 3; i++) {
//      // Write 10 files
//      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
//      System.out.printf("File %d is copied.\n", i);
//    }
//  }
//
//  @Test (timeout = 40000)
//  public void testCopyDelete() throws Exception {
//    waitTillSSMExitSafeMode();
//    MetaStore metaStore = ssm.getMetaStore();
//    // metaStore.deleteAllFileDiff();
//    // metaStore.deleteAllFileInfo();
//    // metaStore.deleteAllCmdlets();
//    // metaStore.deleteAllActions();
//    DistributedFileSystem dfs = cluster.getFileSystem();
//    final String srcPath = "/src/";
//    final String destPath = "/dest/";
//    dfs.mkdirs(new Path(srcPath));
//    dfs.mkdirs(new Path(destPath));
//    // Write to src
//    for (int i = 0; i < 3; i++) {
//      // Create test files
//      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
//      dfs.delete(new Path(srcPath + i), false);
//    }
//
//    Thread.sleep(2000);
//    CmdletManager cmdletManager = ssm.getCmdletManager();
//    // Submit sync action
//    for (int i = 0; i < 3; i++) {
//      // Create test files
//      cmdletManager.submitCmdlet("sync -file /src/" + i + " -src " + srcPath + " -dest " + destPath);
//    }
//    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
//    Assert.assertTrue(actionInfos.size() == 3);
//    Thread.sleep(3000);
//    for (int i = 0; i < 3; i++) {
//      // Write 10 files
//      Assert.assertFalse(dfs.exists(new Path(destPath + i)));
//      System.out.printf("File %d is copied.\n", i);
//    }
//  }
//}
