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
package org.smartdata.server.engine;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.smartdata.action.ActionRegistry;
import org.smartdata.conf.SmartConf;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.CmdletStatusUpdate;
import org.smartdata.protocol.message.StatusReport;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.cmdlet.CmdletDispatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCmdletManager extends MiniSmartClusterHarness {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private CmdletDescriptor generateCmdletDescriptor(String cmd) throws Exception {
    CmdletDescriptor cmdletDescriptor = new CmdletDescriptor(cmd);
    cmdletDescriptor.setRuleId(1);
    return cmdletDescriptor;
  }

  @Test
  public void testCreateFromDescriptor() throws Exception {
    waitTillSSMExitSafeMode();
    String cmd =
            "allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; "
                    + "write -file /test -length 1024";
    CmdletDescriptor cmdletDescriptor = generateCmdletDescriptor(cmd);
    List<ActionInfo> actionInfos = ssm.getCmdletManager().createActionInfos(cmdletDescriptor, 0);
    Assert.assertTrue(cmdletDescriptor.getActionSize() == actionInfos.size());
  }

  @Test
  public void testSubmitAPI() throws Exception {
    waitTillSSMExitSafeMode();

    DistributedFileSystem dfs = cluster.getFileSystem();
    Path dir = new Path("/testMoveFile");
    dfs.mkdirs(dir);
    // Move to SSD
    dfs.setStoragePolicy(dir, "HOT");
    FSDataOutputStream out1 = dfs.create(new Path("/testMoveFile/file1"), true, 1024);
    out1.writeChars("/testMoveFile/file1");
    out1.close();
    Path dir3 = new Path("/testCacheFile");
    dfs.mkdirs(dir3);

    Assert.assertTrue(ActionRegistry.supportedActions().size() > 0);
    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cmdId = cmdletManager.submitCmdlet(
        "allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; "
            + "write -file /test -length 1024");
    Thread.sleep(1200);
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions(10);
    Assert.assertTrue(actionInfos.size() > 0);

    while (true) {
      CmdletState state = cmdletManager.getCmdletInfo(cmdId).getState();
      if (state == CmdletState.DONE) {
        break;
      }
      Assert.assertFalse(CmdletState.isTerminalState(state));
      System.out.printf("Cmdlet still running.\n");
      Thread.sleep(1000);
    }
    List<CmdletInfo> com = ssm.getMetaStore().getCmdlets(null, null, CmdletState.DONE);
    Assert.assertTrue(com.size() >= 1);
    List<ActionInfo> result = ssm.getMetaStore().getActions(null, null);
    Assert.assertTrue(result.size() == 3);
  }

  @Test
  public void wrongCmdlet() throws Exception {
    waitTillSSMExitSafeMode();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    try {
      cmdletManager.submitCmdlet(
          "allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; bug /bug bug bug");
    } catch (IOException e) {
      System.out.println("Wrong cmdlet is detected!");
      Assert.assertTrue(true);
    }
    Thread.sleep(1200);
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions(10);
    Assert.assertTrue(actionInfos.size() == 0);
  }

  @Test
  public void testGetListDeleteCmdlet() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    String cmd =
            "allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; "
                    + "write -file /test -length 1024";
    CmdletDescriptor cmdletDescriptor = generateCmdletDescriptor(cmd);
    CmdletInfo cmdletInfo =
        new CmdletInfo(
            0,
            cmdletDescriptor.getRuleId(),
            CmdletState.PENDING,
            cmdletDescriptor.getCmdletString(),
            123178333L,
            232444994L);
    CmdletInfo[] cmdlets = {cmdletInfo};
    metaStore.insertCmdlets(cmdlets);

    CmdletManager cmdletManager = ssm.getCmdletManager();
    Assert.assertTrue(cmdletManager.listCmdletsInfo(1, null).size() == 1);
    Assert.assertTrue(cmdletManager.getCmdletInfo(0) != null);
    cmdletManager.deleteCmdlet(0);
    Assert.assertTrue(cmdletManager.listCmdletsInfo(1, null).size() == 0);
  }

  @Test
  public void testFileLock() throws Exception {
    waitTillSSMExitSafeMode();
    ssm.getCmdletManager()
        .submitCmdlet("allssd -file /testMoveFile/file1 ; cache -file /testCacheFile");

    thrown.expect(IOException.class);
    ssm.getCmdletManager()
        .submitCmdlet("onessd -file /testMoveFile/file1 ; uncache -file /testCacheFile");
  }

  @Test
  public void testWithoutCluster() throws MetaStoreException, IOException, InterruptedException {
    long cmdletId = 10;
    long actionId = 101;
    MetaStore metaStore = mock(MetaStore.class);
    Assert.assertNotNull(metaStore);
    when(metaStore.getMaxCmdletId()).thenReturn(cmdletId);
    when(metaStore.getMaxActionId()).thenReturn(actionId);
    CmdletDispatcher dispatcher = mock(CmdletDispatcher.class);
    Assert.assertNotNull(dispatcher);
    when(dispatcher.canDispatchMore()).thenReturn(true);
    ServerContext serverContext = new ServerContext(new SmartConf(), metaStore);
    CmdletManager cmdletManager = new CmdletManager(serverContext);
    cmdletManager.init();
    cmdletManager.setDispatcher(dispatcher);

    cmdletManager.start();
    cmdletManager.submitCmdlet("echo");
    Thread.sleep(500);
    verify(metaStore, times(1)).insertCmdlets(any(CmdletInfo[].class));
    verify(metaStore, times(1)).insertActions(any(ActionInfo[].class));
    Thread.sleep(500);

    long startTime = System.currentTimeMillis();
    ActionStatus actionStatus = new ActionStatus(cmdletId, true, actionId, startTime, null);
    StatusReport statusReport = new StatusReport(Arrays.asList(actionStatus));
    cmdletManager.updateStatus(statusReport);
    ActionInfo actionInfo = cmdletManager.getActionInfo(actionId);
    CmdletInfo cmdletInfo = cmdletManager.getCmdletInfo(cmdletId);
    Assert.assertNotNull(actionInfo);

    long finishTime = System.currentTimeMillis();
    actionStatus = new ActionStatus(cmdletId, true, actionId, null, startTime,
        finishTime, null, true);
    statusReport = new StatusReport(Arrays.asList(actionStatus));
    cmdletManager.updateStatus(statusReport);
    Assert.assertTrue(actionInfo.isFinished());
    Assert.assertTrue(actionInfo.isSuccessful());
    Assert.assertEquals(actionInfo.getCreateTime(), startTime);
    Assert.assertEquals(actionInfo.getFinishTime(), finishTime);
    Assert.assertEquals(cmdletInfo.getState(), CmdletState.DONE);

    cmdletManager.updateStatus(
        new CmdletStatusUpdate(cmdletId, System.currentTimeMillis(), CmdletState.EXECUTING));
    CmdletInfo info = cmdletManager.getCmdletInfo(cmdletId);
    Assert.assertNotNull(info);
    Assert.assertEquals(info.getParameters(), "echo");
    Assert.assertEquals(info.getAids().size(), 1);
    Assert.assertTrue(info.getAids().get(0) == actionId);
    Assert.assertEquals(info.getState(), CmdletState.EXECUTING);

    cmdletManager.updateStatus(
        new CmdletStatusUpdate(cmdletId, System.currentTimeMillis(), CmdletState.DONE));
    Assert.assertEquals(info.getState(), CmdletState.DONE);
    Thread.sleep(500);
    verify(metaStore, times(2)).insertCmdlets(any(CmdletInfo[].class));
    verify(metaStore, times(2)).insertActions(any(ActionInfo[].class));

    cmdletManager.stop();
  }

  @Test(timeout = 40000)
  public void testReloadCmdletsInDB() throws Exception {
    waitTillSSMExitSafeMode();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    // Stop cmdletmanager
//    cmdletManager.stop();
    cmdletManager.setTimeout(1000);
    MetaStore metaStore = ssm.getMetaStore();
    String cmd = "write -file /test -length 1024; read -file /test";
    CmdletDescriptor cmdletDescriptor = generateCmdletDescriptor(cmd);
    long submitTime = System.currentTimeMillis();
    CmdletInfo cmdletInfo0 =
        new CmdletInfo(
            0,
            cmdletDescriptor.getRuleId(),
            CmdletState.DISPATCHED,
            cmdletDescriptor.getCmdletString(),
            submitTime,
            submitTime);
    CmdletInfo cmdletInfo1 =
        new CmdletInfo(
            1,
            cmdletDescriptor.getRuleId(),
            CmdletState.PENDING,
            cmdletDescriptor.getCmdletString(),
            submitTime,
            submitTime);
    List<ActionInfo> actionInfos0 =
            cmdletManager.createActionInfos(cmdletDescriptor, cmdletInfo0.getCid());
    flushToDB(metaStore, actionInfos0, cmdletInfo0);
    List<ActionInfo> actionInfos1 =
            cmdletManager.createActionInfos(cmdletDescriptor, cmdletInfo1.getCid());
    flushToDB(metaStore, actionInfos1, cmdletInfo1);
    // init cmdletmanager
    cmdletManager.init();
//    cmdletManager.start();
    CmdletInfo cmdlet0 = cmdletManager.getCmdletInfo(cmdletInfo0.getCid());
    CmdletInfo cmdlet1 = cmdletManager.getCmdletInfo(cmdletInfo1.getCid());
    while (cmdlet0.getState() != CmdletState.FAILED && cmdlet1.getState() != CmdletState.DONE) {
      Thread.sleep(100);
    }
  }

  public void flushToDB(MetaStore metaStore,
                        List<ActionInfo> actionInfos, CmdletInfo cmdletInfo) throws Exception{
    for (ActionInfo actionInfo: actionInfos) {
      cmdletInfo.addAction(actionInfo.getActionId());
    }
    metaStore.insertCmdlet(cmdletInfo);
    metaStore.insertActions(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
  }
}
