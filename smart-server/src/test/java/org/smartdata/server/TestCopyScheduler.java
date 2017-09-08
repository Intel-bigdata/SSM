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

public class TestCopyScheduler extends MiniSmartClusterHarness {

//  @Test
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
//  @Test
//  public void testSyncScheduler() throws Exception {
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
//    Assert.assertTrue(actionInfos.size() == 3);
//    Thread.sleep(3000);
//    for (int i = 0; i < 3; i++) {
//      // Write 10 files
//      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
//      System.out.printf("File %d is copied.\n", i);
//    }
//  }
//
//  @Test
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
}
