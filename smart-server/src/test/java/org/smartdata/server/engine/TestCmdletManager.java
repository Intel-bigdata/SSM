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
import org.smartdata.actions.ActionRegistry;
import org.smartdata.common.CmdletState;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.common.message.ActionFinished;
import org.smartdata.common.message.ActionStarted;
import org.smartdata.common.message.CmdletStatusUpdate;
import org.smartdata.common.models.ActionInfo;
import org.smartdata.common.models.CmdletInfo;
import org.smartdata.conf.SmartConf;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.server.TestEmptyMiniSmartCluster;
import org.smartdata.server.engine.cmdlet.CmdletDispatcher;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;

import java.io.IOException;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestCmdletManager extends TestEmptyMiniSmartCluster {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private CmdletDescriptor generateCmdletDescriptor() throws Exception {
    String cmd = "allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; write -file /test -length 1024";
    CmdletDescriptor cmdletDescriptor = new CmdletDescriptor(cmd);
    cmdletDescriptor.setRuleId(1);
    return cmdletDescriptor;
  }

  @Test
  public void testCreateFromDescriptor() throws Exception {
    waitTillSSMExitSafeMode();
    CmdletDescriptor cmdletDescriptor = generateCmdletDescriptor();
    List<ActionInfo> actionInfos = ssm.getCmdletManager().createActionInfos(cmdletDescriptor, 0);
    Assert.assertTrue(cmdletDescriptor.actionSize() == actionInfos.size());
  }

  @Test
  public void testAPI() throws Exception {
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
    cmdletManager.submitCmdlet("allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; write -file /test -length 1024");
    Thread.sleep(1200);
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions(10);
    Assert.assertTrue(actionInfos.size() >= 0);

    while (true) {
      Thread.sleep(2000);
      int current = cmdletManager.getCmdletsSizeInCache();
      System.out.printf("Current running cmdlet number: %d\n", current);
      if (current == 0) {
        break;
      }
    }

    List<CmdletInfo> com = ssm.getMetaStore().getCmdletsTableItem(null,
      null, CmdletState.DONE);
    Assert.assertTrue(com.size() == 1);
    Assert.assertTrue(com.get(0).getState() == CmdletState.DONE);
    List<ActionInfo> result = ssm.getMetaStore().getActionsTableItem(null, null);
    Assert.assertTrue(result.size() == 3);
  }

  @Test
  public void wrongCmdlet() throws Exception {
    waitTillSSMExitSafeMode();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    try {
      cmdletManager.submitCmdlet("allssd -file /testMoveFile/file1 ; cache -file /testCacheFile ; bug /bug bug bug");
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
    CmdletDescriptor cmdletDescriptor = generateCmdletDescriptor();
    CmdletInfo cmdletInfo = new CmdletInfo(0, cmdletDescriptor.getRuleId(),
      CmdletState.PENDING, cmdletDescriptor.getCmdletString(),
      123178333l, 232444994l);
    CmdletInfo[] cmdlets = {cmdletInfo};
    metaStore.insertCmdletsTable(cmdlets);

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
    when(metaStore.getMaxCmdletId()).thenReturn(cmdletId);
    when(metaStore.getMaxActionId()).thenReturn(actionId);
    CmdletDispatcher dispatcher = mock(CmdletDispatcher.class);
    when(dispatcher.canDispatchMore()).thenReturn(true);
    ServerContext serverContext = new ServerContext(new SmartConf(), metaStore);
    CmdletManager cmdletManager = new CmdletManager(serverContext);
    cmdletManager.init();
    cmdletManager.setDispatcher(dispatcher);

    cmdletManager.start();
    cmdletManager.submitCmdlet("hello");
    verify(metaStore, times(1)).insertCmdletTable(any(CmdletInfo.class));
    verify(metaStore, times(1)).insertActionsTable(any(ActionInfo[].class));

    Assert.assertEquals(1, cmdletManager.getCmdletsSizeInCache());
    Thread.sleep(1000);
    verify(dispatcher, times(1)).dispatch(any(LaunchCmdlet.class));

    long actionStartTime = System.currentTimeMillis();
    cmdletManager.updateStatus(new ActionStarted(actionId, actionStartTime));
    ActionInfo actionInfo = cmdletManager.getActionInfo(actionId);
    Assert.assertNotNull(actionInfo);
    Assert.assertEquals(actionInfo.getCreateTime(), actionStartTime);

    long actionFinished = System.currentTimeMillis();
    cmdletManager.updateStatus(new ActionFinished(actionId, actionFinished, null));
    Assert.assertTrue(actionInfo.isFinished());
    Assert.assertTrue(actionInfo.isSuccessful());
    Assert.assertEquals(actionInfo.getFinishTime(), actionFinished);

    cmdletManager.updateStatus(new CmdletStatusUpdate(cmdletId, System.currentTimeMillis(), CmdletState.EXECUTING));
    CmdletInfo info = cmdletManager.getCmdletInfo(cmdletId);
    Assert.assertNotNull(info);
    Assert.assertEquals(info.getParameters(), "hello");
    Assert.assertEquals(info.getAids().size(), 1);
    Assert.assertTrue(info.getAids().get(0) == actionId);
    Assert.assertEquals(info.getState(), CmdletState.EXECUTING);

    cmdletManager.updateStatus(new CmdletStatusUpdate(cmdletId, System.currentTimeMillis(), CmdletState.DONE));
    Assert.assertEquals(info.getState(), CmdletState.DONE);
    Assert.assertEquals(0, cmdletManager.getCmdletsSizeInCache());

    Assert.assertNull(cmdletManager.getCmdletInfo(cmdletId));
    Assert.assertNull(cmdletManager.getActionInfo(actionId));

    cmdletManager.stop();
  }
}
