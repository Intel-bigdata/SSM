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
package org.smartdata.metastore;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.metastore.utils.TestDaoUtil;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CachedFileStatus;
import org.smartdata.model.ClusterConfig;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileInfo;
import org.smartdata.model.GlobalConfig;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.model.StorageCapacity;
import org.smartdata.model.StoragePolicy;
import org.smartdata.model.XAttribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class TestMetaStore extends TestDaoUtil {
  private MetaStore metaStore;

  @Before
  public void metaInit() throws Exception {
    initDao();
    metaStore = new MetaStore(druidPool);
  }

  @After
  public void metaClose() throws Exception {
    closeDao();
    if (metaStore != null) {
      metaStore = null;
    }
  }

  @Test
  public void testHighConcurrency() throws Exception {
    // Multiple threads
    Thread th1 = new InsertThread(metaStore);
    Thread th2 = new SelectUpdateThread(metaStore);
    th1.start();
    Thread.sleep(1000);
    th2.start();
    th2.join();
  }

  @Test
  public void testThreadSleepConcurrency() throws Exception {
    // Multiple threads
    Thread th1 = new InsertThread(metaStore);
    Thread th2 = new SleepSelectUpdateThread(metaStore);
    th1.start();
    Thread.sleep(1000);
    th2.start();
    th2.join();
  }

  class SleepSelectUpdateThread extends Thread {
    private MetaStore metaStore;

    public SleepSelectUpdateThread(MetaStore metaStore) {
      this.metaStore = metaStore;
    }

    public void run() {
      for (int i = 0; i < 100; i++) {
        try {
          List<ActionInfo> actionInfoList = metaStore
              .getActionsTableItem(Arrays.asList(new Long[]{(long) i}));
          actionInfoList.get(0).setFinished(true);
          actionInfoList.get(0).setFinishTime(System.currentTimeMillis());
          sleep(5);
          metaStore.updateActionsTable(
              actionInfoList.toArray(new ActionInfo[actionInfoList.size()]));
          metaStore.getActionsTableItem(null, null);
        } catch (MetaStoreException e) {
          System.out.println(e.getMessage());
          Assert.assertTrue(false);
        } catch (InterruptedException e) {
          System.out.println(e.getMessage());
          Assert.assertTrue(false);
        }
      }
    }
  }


  class InsertThread extends Thread {
    private MetaStore metaStore;

    public InsertThread(MetaStore metaStore) {
      this.metaStore = metaStore;
    }

    public void run() {
      Map<String, String> args = new HashMap();
      ActionInfo actionInfo = new ActionInfo(1, 1,
          "cache", args, "Test",
          "Test", true, 123213213l, true, 123123l,
          100);
      for (int i = 0; i < 100; i++) {
        actionInfo.setActionId(i);
        try {
          metaStore.insertActionTable(actionInfo);
        } catch (MetaStoreException e) {
          System.out.println(e.getMessage());
          Assert.assertTrue(false);
        }
      }
    }
  }

  class SelectUpdateThread extends Thread {
    private MetaStore metaStore;

    public SelectUpdateThread(MetaStore metaStore) {
      this.metaStore = metaStore;
    }

    public void run() {
      for (int i = 0; i < 100; i++) {
        try {
          List<ActionInfo> actionInfoList = metaStore.getActionsTableItem(Arrays.asList(new Long[]{(long) i}));
          actionInfoList.get(0).setFinished(true);
          actionInfoList.get(0).setFinishTime(System.currentTimeMillis());
          metaStore.updateActionsTable(actionInfoList.toArray(new ActionInfo[actionInfoList.size()]));
          metaStore.getActionsTableItem(null, null);
        } catch (MetaStoreException e) {
          System.out.println(e.getMessage());
          Assert.assertTrue(false);
        }
      }
    }
  }

  @Test
  public void testGetFiles() throws Exception {
    String pathString = "/tmp/des";
    long length = 123L;
    boolean isDir = false;
    int blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    String owner = "root";
    String group = "admin";
    long fileId = 56l;
    byte storagePolicy = 0;
    FileInfo fileInfo = new FileInfo(pathString, fileId, length,
        isDir, (short) blockReplication, blockSize, modTime, accessTime,
        (short) 1, owner, group, storagePolicy);
    metaStore.insertFile(fileInfo);
    FileInfo dbFileInfo = metaStore.getFile(56);
    Assert.assertTrue(dbFileInfo.equals(fileInfo));
    dbFileInfo = metaStore.getFile("/tmp/des");
    Assert.assertTrue(dbFileInfo.equals(fileInfo));
  }

  @Test
  public void testGetNonExistFile() throws Exception {
    FileInfo info = metaStore.getFile("/non_exist_file_path");
    Assert.assertTrue(info == null);
  }

  @Test
  public void testInsertStoragesTable() throws Exception {
    StorageCapacity storage1 = new StorageCapacity("Flash",
        12343333l, 2223333l);
    StorageCapacity storage2 = new StorageCapacity("RAM",
        12342233l, 2223663l);
    StorageCapacity[] storages = {storage1, storage2};
    metaStore.insertStoragesTable(storages);
    StorageCapacity storageCapacity1 = metaStore
        .getStorageCapacity("Flash");
    StorageCapacity storageCapacity2 = metaStore
        .getStorageCapacity("RAM");
    Assert.assertTrue(storageCapacity1.equals(storage1));
    Assert.assertTrue(storageCapacity2.equals(storage2));
    Assert.assertTrue(metaStore.updateStoragesTable("Flash",
        123456L, 4562233L));
    Assert.assertTrue(metaStore.getStorageCapacity("Flash")
        .getCapacity() == 12343333l);
  }


  @Test
  public void testGetStorageCapacity() throws Exception {
    StorageCapacity storage1 = new StorageCapacity("HDD",
        12343333l, 2223333l);
    StorageCapacity storage2 = new StorageCapacity("RAM",
        12342233l, 2223663l);
    StorageCapacity[] storages = {storage1, storage2};
    metaStore.insertStoragesTable(storages);
    Assert.assertTrue(metaStore.getStorageCapacity("HDD").equals(storage1));
    Assert.assertTrue(metaStore.getStorageCapacity("RAM").equals(storage2));
  }

  @Test
  public void testInsertRule() throws Exception {
    String rule = "file : accessCount(10m) > 20 \n\n"
        + "and length() > 3 | cache";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(0, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    Assert.assertTrue(metaStore.insertNewRule(info1));
    RuleInfo info1_1 = metaStore.getRuleInfo(info1.getId());
    Assert.assertTrue(info1.equals(info1_1));

    long now = System.currentTimeMillis();
    metaStore.updateRuleInfo(info1.getId(), RuleState.DELETED, now, 1, 1);
    info1.setState(RuleState.DELETED);
    info1.setLastCheckTime(now);
    info1.setNumChecked(1);
    info1.setNumCmdsGen(1);
    RuleInfo info1_2 = metaStore.getRuleInfo(info1.getId());
    Assert.assertTrue(info1_2.equals(info1));

    RuleInfo info2 = new RuleInfo(0, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    Assert.assertTrue(metaStore.insertNewRule(info2));
    RuleInfo info2_1 = metaStore.getRuleInfo(info2.getId());
    Assert.assertFalse(info1_1.equals(info2_1));

    List<RuleInfo> infos = metaStore.getRuleInfo();
    Assert.assertTrue(infos.size() == 2);
  }

  @Test
  public void testUpdateCachedFiles() throws Exception {
    metaStore.insertCachedFiles(80L, "testPath", 1000L,
        2000L, 100);
    metaStore.insertCachedFiles(90L, "testPath2", 2000L,
        3000L, 200);
    Map<String, Long> pathToId = new HashMap<>();
    pathToId.put("testPath", 80L);
    pathToId.put("testPath2", 90L);
    pathToId.put("testPath3", 100L);
    List<FileAccessEvent> events = new ArrayList<>();
    events.add(new FileAccessEvent("testPath", 3000L));
    events.add(new FileAccessEvent("testPath", 4000L));
    events.add(new FileAccessEvent("testPath2", 4000L));
    events.add(new FileAccessEvent("testPath2", 5000L));

    events.add(new FileAccessEvent("testPath3", 8000L));
    events.add(new FileAccessEvent("testPath3", 9000L));

    metaStore.updateCachedFiles(pathToId, events);
    List<CachedFileStatus> statuses = metaStore.getCachedFileStatus();
    Assert.assertTrue(statuses.size() == 2);
    Map<Long, CachedFileStatus> statusMap = new HashMap<>();
    for (CachedFileStatus status : statuses) {
      statusMap.put(status.getFid(), status);
    }
    Assert.assertTrue(statusMap.containsKey(80L));
    CachedFileStatus first = statusMap.get(80L);
    Assert.assertTrue(first.getLastAccessTime() == 4000L);
    Assert.assertTrue(first.getNumAccessed() == 102);

    Assert.assertTrue(statusMap.containsKey(90L));
    CachedFileStatus second = statusMap.get(90L);
    Assert.assertTrue(second.getLastAccessTime() == 5000L);
    Assert.assertTrue(second.getNumAccessed() == 202);
  }


  @Test
  public void testInsertDeleteCachedFiles() throws Exception {
    metaStore.insertCachedFiles(80l, "testPath", 123456l,
        234567l, 456);
    Assert.assertTrue(metaStore.getCachedFileStatus(
        80l).getFromTime() == 123456l);
    // Update record with 80l id
    Assert.assertTrue(metaStore.updateCachedFiles(80l,
        234568l, 460));
    Assert.assertTrue(metaStore.getCachedFileStatus().get(0)
        .getLastAccessTime() == 234568l);
    List<CachedFileStatus> list = new LinkedList<>();
    list.add(new CachedFileStatus(321l, "testPath", 113334l,
        222222l, 222));
    metaStore.insertCachedFiles(list);
    Assert.assertTrue(metaStore.getCachedFileStatus(321l)
        .getNumAccessed() == 222);
    Assert.assertTrue(metaStore.getCachedFileStatus().size() == 2);
    // Delete one record
    metaStore.deleteCachedFile(321l);
    Assert.assertTrue(metaStore.getCachedFileStatus().size() == 1);
    // Clear all records
    metaStore.deleteAllCachedFile();
    Assert.assertTrue(metaStore.getCachedFileStatus().size() == 0);
    metaStore.insertCachedFiles(80l, "testPath", 123456l,
        234567l, 456);
  }

  @Test
  public void testGetCachedFileStatus() throws Exception {
    metaStore.insertCachedFiles(6l, "testPath", 1490918400000l,
        234567l, 456);
    metaStore.insertCachedFiles(19l, "testPath", 1490918400000l,
        234567l, 456);
    metaStore.insertCachedFiles(23l, "testPath", 1490918400000l,
        234567l, 456);
    CachedFileStatus cachedFileStatus = metaStore.getCachedFileStatus(6);
    Assert.assertTrue(cachedFileStatus.getFromTime() == 1490918400000l);
    List<CachedFileStatus> cachedFileList = metaStore.getCachedFileStatus();
    List<Long> fids = metaStore.getCachedFids();
    Assert.assertTrue(fids.size() == 3);
    Assert.assertTrue(cachedFileList.get(0).getFid() == 6);
    Assert.assertTrue(cachedFileList.get(1).getFid() == 19);
    Assert.assertTrue(cachedFileList.get(2).getFid() == 23);
  }

  @Test
  public void testInsetFiles() throws Exception {
    String pathString = "/tmp/testFile";
    long length = 123L;
    boolean isDir = false;
    int blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    String owner = "root";
    String group = "admin";
    long fileId = 312321L;
    byte storagePolicy = 0;
    FileInfo[] files = {new FileInfo(pathString, fileId, length,
        isDir, (short) blockReplication, blockSize, modTime, accessTime,
        (short) 1, owner, group, storagePolicy)};
    metaStore.insertFiles(files);
    FileInfo dbFileInfo = metaStore.getFile("/tmp/testFile");
    Assert.assertTrue(dbFileInfo.equals(files[0]));
  }

  @Test
  public void testInsertCmdletsTable() throws Exception {
    CmdletInfo command1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333l, 232444444l);
    metaStore.insertCmdletTable(command1);
    CmdletInfo command2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333l, 232444994l);
    metaStore.insertCmdletTable(command2);
    String cidCondition = ">= 0 ";
    String ridCondition = "= 78 ";
    CmdletState state = null;
    CmdletState state1 = CmdletState.PAUSED;
    List<CmdletInfo> com = metaStore.getCmdletsTableItem(cidCondition, ridCondition, state);
    Assert.assertTrue(com.get(0).equals(command2));
    List<CmdletInfo> com1 = metaStore.getCmdletsTableItem(null,
        null, state1);
    Assert.assertTrue(com1.get(0).equals(command2));
  }

  @Test
  public void testUpdateDeleteCommand() throws Exception {
    long commandId = 0;
    commandId = metaStore.getMaxCmdletId();
    System.out.printf("CommandID = %d\n", commandId);
    CmdletInfo command1 = new CmdletInfo(0, 1,
        CmdletState.PENDING, "test", 123123333l, 232444444l);
    CmdletInfo command2 = new CmdletInfo(1, 78,
        CmdletState.PENDING, "tt", 123178333l, 232444994l);
    CmdletInfo[] commands = {command1, command2};
    metaStore.insertCmdletsTable(commands);
    commandId = metaStore.getMaxCmdletId();
    String cidCondition = ">= 1 ";
    String ridCondition = "= 78 ";
    List<CmdletInfo> com = metaStore
        .getCmdletsTableItem(cidCondition, ridCondition, CmdletState.PENDING);
    commandId = metaStore.getMaxCmdletId();
    Assert.assertTrue(commandId == commands.length);
    for (CmdletInfo cmd : com) {
      // System.out.printf("Cid = %d \n", cmd.getCid());
      metaStore.updateCmdletStatus(cmd.getCid(), cmd.getRid(), CmdletState.DONE);
    }
    List<CmdletInfo> com1 = metaStore.getCmdletsTableItem(cidCondition, ridCondition, CmdletState.DONE);
    Assert.assertTrue(com1.size() == 1);

    Assert.assertTrue(com1.get(0).getState().equals(CmdletState.DONE));
    metaStore.deleteCmdlet(command2.getCid());
    com1 = metaStore.getCmdletsTableItem(cidCondition, ridCondition, CmdletState.DONE);
    Assert.assertTrue(com1.size() == 0);
  }

  @Test
  public void testInsertListActions() throws Exception {
    Map<String, String> args = new HashMap();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStore.insertActionsTable(new ActionInfo[]{actionInfo});
    List<ActionInfo> actionInfos = metaStore.getActionsTableItem(null, null);
    Assert.assertTrue(actionInfos.size() == 1);
    actionInfo.setResult("Finished");
    metaStore.updateActionsTable(new ActionInfo[]{actionInfo});
    actionInfos = metaStore.getActionsTableItem(null, null);
    Assert.assertTrue(actionInfos.get(0).equals(actionInfo));
  }

  @Test
  public void testGetNewCreatedActions() throws Exception {
    Map<String, String> args = new HashMap();
    List<ActionInfo> actionInfos;
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStore.insertActionTable(actionInfo);
    actionInfo.setActionId(2);
    metaStore.insertActionTable(actionInfo);
    actionInfos = metaStore.getNewCreatedActionsTableItem(1);
    Assert.assertTrue(actionInfos.size() == 1);
    actionInfos = metaStore.getNewCreatedActionsTableItem(2);
    Assert.assertTrue(actionInfos.size() == 2);
  }

  @Test
  public void testGetMaxActionId() throws Exception {
    long currentId = metaStore.getMaxActionId();
    Map<String, String> args = new HashMap();
    Assert.assertTrue(currentId == 0);
    ActionInfo actionInfo = new ActionInfo(currentId, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStore.insertActionsTable(new ActionInfo[]{actionInfo});
    currentId = metaStore.getMaxActionId();
    Assert.assertTrue(currentId == 1);
    actionInfo = new ActionInfo(currentId, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStore.insertActionsTable(new ActionInfo[]{actionInfo});
    currentId = metaStore.getMaxActionId();
    Assert.assertTrue(currentId == 2);
  }

  @Test
  public void testInsertStoragePolicyTable() throws Exception {
    metaStore.insertStoragePolicyTable(new StoragePolicy((byte) 3, "COOL"));
    metaStore.insertStoragePolicyTable(new StoragePolicy((byte) 2, "COLD"));
    String value = metaStore.getStoragePolicyName(3);
    Assert.assertEquals(metaStore.getStoragePolicyName(2), "COLD");
    int key = metaStore.getStoragePolicyID("COOL");
    Assert.assertEquals(value, "COOL");
    Assert.assertEquals(key, 3);
  }

  @Test
  public void testInsertXattrTable() throws Exception {
    long fid = 567l;
    List<XAttribute> attributes = new ArrayList<>();
    Random random = new Random();
    byte[] value1 = new byte[1024];
    byte[] value2 = new byte[1024];
    random.nextBytes(value1);
    random.nextBytes(value2);
    attributes.add(new XAttribute("user", "a1", value1));
    attributes.add(new XAttribute("raw", "you", value2));
    Assert.assertTrue(metaStore.insertXattrList(fid, attributes));
    List<XAttribute> result = metaStore.getXattrList(fid);
    Assert.assertTrue(result.size() == attributes.size());
    Assert.assertTrue(result.containsAll(attributes));
  }

  @Test
  public void testSetClusterConfig() throws MetaStoreException {
    ClusterConfig clusterConfig = new ClusterConfig(1,"test" , "test1");
    metaStore.setClusterConfig(clusterConfig);
    List<ClusterConfig> list = new LinkedList<>();
    list.add(clusterConfig);
    Assert.assertTrue(metaStore.listClusterConfig().equals(list));
    list.get(0).setConfig_path("test2");

    metaStore.setClusterConfig(list.get(0));
    Assert.assertTrue(metaStore.listClusterConfig().equals(list));
  }

  @Test
  public void testDelClusterConfig() throws MetaStoreException {
    ClusterConfig clusterConfig = new ClusterConfig(1, "test", "test1");
    metaStore.setClusterConfig(clusterConfig);
    metaStore.delClusterConfig(clusterConfig);
    Assert.assertTrue(metaStore.listClusterConfig().size() == 0);
  }

  @Test
  public void testSetGlobalConfig() throws MetaStoreException {
    GlobalConfig globalConfig = new GlobalConfig(1,"test" , "test1");
    metaStore.setGlobalConfig(globalConfig);
    Assert.assertTrue(metaStore.getDefaultGlobalConfigByName("test").equals(globalConfig));
    globalConfig.setPropertyValue("test2");

    metaStore.setGlobalConfig(globalConfig);
    Assert.assertTrue(metaStore.getDefaultGlobalConfigByName("test").equals(globalConfig));
  }

}
