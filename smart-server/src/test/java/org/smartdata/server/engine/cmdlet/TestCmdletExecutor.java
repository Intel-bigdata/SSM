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
//package org.smartdata.server.engine.cmdlet;
//
//import org.smartdata.actions.ActionRegistry;
//import org.smartdata.common.CmdletState;
//import org.smartdata.models.ActionInfo;
//import org.smartdata.common.cmdlet.CmdletDescriptor;
//import org.smartdata.models.CmdletInfo;
//import org.smartdata.metastore.MetaStore;
//import org.smartdata.server.TestEmptyMiniSmartCluster;
//
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
//
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * CmdletExecutor Unit Test
// */
//public class TestCmdletExecutor extends TestEmptyMiniSmartCluster {
//
//  @Test
//  public void testCreateFromDescriptor() throws Exception {
//    waitTillSSMExitSafeMode();
//    generateTestCases();
//    CmdletDescriptor cmdletDescriptor = generateCmdletDescriptor();
//    List<ActionInfo> actionInfos = ssm.getCmdletExecutor().createActionInfos(cmdletDescriptor, 0);
//    Assert.assertTrue(cmdletDescriptor.actionSize() == actionInfos.size());
//  }
//
//
//  @Test
//  public void testfileLock() throws Exception {
//    waitTillSSMExitSafeMode();
//    // generateTestCases();
//    ssm.getCmdletExecutor()
//        .submitCmdlet("allssd -file /testMoveFile/file1 ; cache -file /testCacheFile");
//    // Cause Exception with the same files
//    try {
//      ssm.getCmdletExecutor()
//          .submitCmdlet("onessd -file /testMoveFile/file1 ; uncache -file /testCacheFile");
//    } catch (IOException e) {
//      Assert.assertTrue(true);
//    }
//  }
//
// /* @Test
//  public void testCmdletExecutor() throws Exception {
//    waitTillSSMExitSafeMode();
//    generateTestFiles();
//    generateTestCases();
//    testCmdletExecutorHelper();
//  }*/
//
//  @Test
//  public void testAPI() throws Exception {
//    waitTillSSMExitSafeMode();
//    generateTestFiles();
//    Assert.assertTrue(ActionRegistry.supportedActions().size() > 0);
//    ssm.getCmdletExecutor()
//        .submitCmdlet("allssd -file /testMoveFile/file1 ; "
//            + "cache -file /testCacheFile ; "
//            + "write -file /test -length 1024");
//
//    List<ActionInfo> actionInfos = new ArrayList<>();
//    for (int i = 0; i < 4; i++) {
//      Thread.sleep(1000);
//      actionInfos = ssm.getCmdletExecutor().listNewCreatedActions(10);
//      System.out.println((i + 1) + "s, actions = " + actionInfos.size());
//      if (actionInfos.size() > 0) {
//        break;
//      }
//    }
//    Assert.assertTrue(actionInfos.size() > 0);
//    testCmdletExecutorHelper();
//  }
//
//  @Test
//  public void wrongCmdlet() throws Exception {
//    waitTillSSMExitSafeMode();
//    generateTestFiles();
//    Assert.assertTrue(ActionRegistry.supportedActions().size() > 0);
//    try {
//      ssm.getCmdletExecutor()
//          .submitCmdlet("allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; bug /bug bug bug");
//    } catch (IOException e) {
//      System.out.println("Wrong cmdlet is detected!");
//      Assert.assertTrue(true);
//    }
//    Thread.sleep(1200);
//    List<ActionInfo> actionInfos = ssm.getCmdletExecutor().listNewCreatedActions(10);
//    Assert.assertTrue(actionInfos.size() == 0);
//    // testCmdletExecutorHelper();
//  }
//
//  @Test
//  public void testGetListDeleteCmdlet() throws Exception {
//    waitTillSSMExitSafeMode();
//    generateTestCases();
//    Assert.assertTrue(ssm
//        .getCmdletExecutor()
//        .listCmdletsInfo(1, null).size() == 1);
//    Assert.assertTrue(ssm
//        .getCmdletExecutor().getCmdletInfo(0) != null);
//    ssm.getCmdletExecutor().deleteCmdlet(0);
//    Assert.assertTrue(ssm
//        .getCmdletExecutor()
//        .listCmdletsInfo(1, null).size() == 0);
//    // Test clear all cmdlets under rid = 1
//    ssm.getCmdletExecutor().submitCmdlet(
//        new CmdletDescriptor("allssd -file /testMoveFile/file1", 1));
//    ssm.getCmdletExecutor().submitCmdlet(
//        new CmdletDescriptor("write -file /test1 -length 1024", 1));
//    ssm.getCmdletExecutor().submitCmdlet(
//        new CmdletDescriptor("write -file /test -length 1024", 1));
//    Assert.assertTrue(ssm
//        .getCmdletExecutor()
//        .listCmdletsInfo(1, null).size() == 3);
//    ssm.getCmdletExecutor().deleteCmdletByRule(1);
//    Assert.assertTrue(ssm
//        .getCmdletExecutor()
//        .listCmdletsInfo(1, null).size() == 0);
//  }
//
///*  @Test
//  public void testActivateDisableCmdlet() throws Exception {
//    waitTillSSMExitSafeMode();
//    generateTestFiles();
//    generateTestCases();
//    // Activate 1
//    ssm.getCmdletExecutor().activateCmdlet(1);
//    Assert.assertTrue(ssm.getCmdletExecutor().inCache(1));
//    // Disable 1
//    CmdletInfo cmdinfo = ssm.getCmdletExecutor().getCmdletInfo(1);
//    if (cmdinfo.getState() != CmdletState.DONE) {
//      ssm.getCmdletExecutor().disableCmdlet(1);
//      Assert.assertTrue(cmdinfo.getState() == CmdletState.DISABLED);
//    }
//  }*/
//
//  private void generateTestFiles() throws IOException {
//    final DistributedFileSystem dfs = cluster.getFileSystem();
//    // New dir
//    Path dir = new Path("/testMoveFile");
//    dfs.mkdirs(dir);
//    // Move to SSD
//    dfs.setStoragePolicy(dir, "HOT");
//    final FSDataOutputStream out1 = dfs.create(new Path("/testMoveFile/file1"),
//        true, 1024);
//    out1.writeChars("/testMoveFile/file1");
//    out1.close();
//    // Move to Archive
//    // final FSDataOutputStream out2 = dfs.create(new Path("/testMoveFile/file2"),
//    //     true, 1024);
//    // out2.writeChars("/testMoveFile/file2");
//    // out2.close();
//    // Move to CacheObject
//    Path dir3 = new Path("/testCacheFile");
//    dfs.mkdirs(dir3);
//  }
//
//  private CmdletDescriptor generateCmdletDescriptor() throws Exception {
//    String cmd = "allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; write -file /test -length 1024";
//    CmdletDescriptor cmdletDescriptor = new CmdletDescriptor(cmd);
//    cmdletDescriptor.setRuleId(1);
//    return cmdletDescriptor;
//  }
//
//  private void generateTestCases() throws Exception {
//    MetaStore metaStore = ssm.getCmdletExecutor().getContext().getMetaStore();
//    CmdletDescriptor cmdletDescriptor = generateCmdletDescriptor();
//    CmdletInfo cmdletInfo = new CmdletInfo(0, cmdletDescriptor.getRuleId(),
//        CmdletState.PENDING, cmdletDescriptor.getCmdletString(),
//        123178333l, 232444994l);
//    CmdletInfo[] cmdlets = {cmdletInfo};
//    metaStore.insertCmdletsTable(cmdlets);
//  }
//
//  private void testCmdletExecutorHelper() throws Exception {
//    MetaStore metaStore = ssm.getCmdletExecutor().getContext().getMetaStore();
//    while (true) {
//      Thread.sleep(2000);
//      int current = ssm.getCmdletExecutor().cacheSize();
//      System.out.printf("Cmdlet CacheObject size = %d\n", current);
//      if (current == 0) {
//        break;
//      }
//    }
//    List<CmdletInfo> com = metaStore.getCmdlets(null,
//        null, CmdletState.DONE);
//    System.out.printf("CmdletInfos Size = %d\n", com.size());
//    // Check Status
//    Assert.assertTrue(com.size() == 1);
//    Assert.assertTrue(com.get(0).getState() == CmdletState.DONE);
//    List<ActionInfo> actionInfos = metaStore
//        .getActions(null, null);
//    System.out.printf("ActionInfos Size = %d\n", actionInfos.size());
//    Assert.assertTrue(actionInfos.size() == 3);
//  }
//}
