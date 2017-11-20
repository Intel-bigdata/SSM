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
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.CachedFileStatus;
import org.smartdata.model.ClusterConfig;
import org.smartdata.model.ClusterInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.DataNodeInfo;
import org.smartdata.model.DataNodeStorageInfo;
import org.smartdata.model.FileInfo;
import org.smartdata.model.GlobalConfig;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.model.StorageCapacity;
import org.smartdata.model.StoragePolicy;
import org.smartdata.model.SystemInfo;
import org.smartdata.model.XAttribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
          List<ActionInfo> actionInfoList =
              metaStore.getActions(Arrays.asList((long) i));
          actionInfoList.get(0).setFinished(true);
          actionInfoList.get(0).setFinishTime(System.currentTimeMillis());
          sleep(5);
          metaStore.updateActions(actionInfoList.toArray(new ActionInfo[actionInfoList.size()]));
          metaStore.getActions(null, null);
        } catch (MetaStoreException | InterruptedException e) {
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
      ActionInfo actionInfo =
          new ActionInfo(1, 1, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
      for (int i = 0; i < 100; i++) {
        actionInfo.setActionId(i);
        try {
          metaStore.insertAction(actionInfo);
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
          List<ActionInfo> actionInfoList =
              metaStore.getActions(Arrays.asList((long) i));
          actionInfoList.get(0).setFinished(true);
          actionInfoList.get(0).setFinishTime(System.currentTimeMillis());
          metaStore.updateActions(actionInfoList.toArray(new ActionInfo[actionInfoList.size()]));
          metaStore.getActions(null, null);
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
    long fileId = 56L;
    byte storagePolicy = 0;
    FileInfo fileInfo =
        new FileInfo(
            pathString,
            fileId,
            length,
            isDir,
            (short) blockReplication,
            blockSize,
            modTime,
            accessTime,
            (short) 1,
            owner,
            group,
            storagePolicy);
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
    StorageCapacity storage1 = new StorageCapacity("Flash", 12343333L, 2223333L);
    StorageCapacity storage2 = new StorageCapacity("RAM", 12342233L, 2223663L);
    StorageCapacity[] storages = {storage1, storage2};
    metaStore.insertUpdateStoragesTable(storages);
    StorageCapacity storageCapacity1 = metaStore.getStorageCapacity("Flash");
    StorageCapacity storageCapacity2 = metaStore.getStorageCapacity("RAM");
    Assert.assertTrue(storageCapacity1.equals(storage1));
    Assert.assertTrue(storageCapacity2.equals(storage2));
    Assert.assertTrue(metaStore.updateStoragesTable("Flash", 123456L, 4562233L));
    Assert.assertTrue(metaStore.getStorageCapacity("Flash").getCapacity() == 12343333L);
  }

  @Test
  public void testGetStoreCapacityOfDifferentStorageType() throws Exception {
    DataNodeStorageInfo info1 = new DataNodeStorageInfo("1", "ssd", 1, "1", 1, 1, 1, 1, 1);
    DataNodeStorageInfo info2 = new DataNodeStorageInfo("2", "ssd", 2, "2", 2, 2, 2, 2, 2);

    metaStore.insertDataNodeStorageInfo(info1);
    metaStore.insertDataNodeStorageInfo(info2);

    long capacity = metaStore.getStoreCapacityOfDifferentStorageType("ssd");
    Assert.assertTrue(capacity == 3);
  }

  @Test
  public void testGetStoreFreeOfDifferentStorageType() throws Exception {
    DataNodeStorageInfo info1 = new DataNodeStorageInfo("1", "ssd", 1, "1", 1, 1, 1, 1, 1);
    DataNodeStorageInfo info2 = new DataNodeStorageInfo("2", "ssd", 2, "2", 2, 2, 2, 2, 2);

    metaStore.insertDataNodeStorageInfo(info1);
    metaStore.insertDataNodeStorageInfo(info2);

    long free = metaStore.getStoreFreeOfDifferentStorageType("ssd");
    Assert.assertTrue(free == 3);
  }

  @Test
  public void testGetStorageCapacity() throws Exception {
    StorageCapacity storage1 = new StorageCapacity("HDD", 12343333L, 2223333L);
    StorageCapacity storage2 = new StorageCapacity("RAM", 12342233L, 2223663L);
    StorageCapacity[] storages = {storage1, storage2};
    metaStore.insertUpdateStoragesTable(storages);
    Assert.assertTrue(metaStore.getStorageCapacity("HDD").equals(storage1));
    Assert.assertTrue(metaStore.getStorageCapacity("RAM").equals(storage2));

    StorageCapacity storage3 = new StorageCapacity("HDD", 100L, 10L);
    metaStore.insertUpdateStoragesTable(storage3);
    Assert.assertTrue(metaStore.getStorageCapacity("HDD").equals(storage3));
  }

  @Test
  public void testInsertRule() throws Exception {
    String rule = "file : accessCount(10m) > 20 \n\n" + "and length() > 3 | cache";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(0, submitTime, rule, RuleState.ACTIVE, 0, 0, 0);
    Assert.assertTrue(metaStore.insertNewRule(info1));
    RuleInfo info11 = metaStore.getRuleInfo(info1.getId());
    Assert.assertTrue(info1.equals(info11));

    long now = System.currentTimeMillis();
    metaStore.updateRuleInfo(info1.getId(), RuleState.DELETED, now, 1, 1);
    info1.setState(RuleState.DELETED);
    info1.setLastCheckTime(now);
    info1.setNumChecked(1);
    info1.setNumCmdsGen(1);
    RuleInfo info12 = metaStore.getRuleInfo(info1.getId());
    Assert.assertTrue(info12.equals(info1));

    RuleInfo info2 = new RuleInfo(0, submitTime, rule, RuleState.ACTIVE, 0, 0, 0);
    Assert.assertTrue(metaStore.insertNewRule(info2));
    RuleInfo info21 = metaStore.getRuleInfo(info2.getId());
    Assert.assertFalse(info11.equals(info21));

    List<RuleInfo> infos = metaStore.getRuleInfo();
    Assert.assertTrue(infos.size() == 2);
  }

  @Test
  public void testMoveSyncRules() throws Exception {
    String pathString = "/src/1";
    long length = 123L;
    boolean isDir = false;
    int blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    String owner = "root";
    String group = "admin";
    long fileId = 56L;
    byte storagePolicy = 0;
    FileInfo fileInfo =
        new FileInfo(
            pathString,
            fileId,
            length,
            isDir,
            (short) blockReplication,
            blockSize,
            modTime,
            accessTime,
            (short) 1,
            owner,
            group,
            storagePolicy);
    metaStore.insertFile(fileInfo);
    Map<String, String> args = new HashMap();
    args.put("-file", "/src/1");
    String rule = "file : accessCount(10m) > 20 \n\n" + "and length() > 3 | ";
    long submitTime = System.currentTimeMillis();
    RuleInfo ruleInfo =
        new RuleInfo(0, submitTime, rule + "sync -dest /dest/", RuleState.ACTIVE, 0, 0, 0);
    metaStore.insertNewRule(ruleInfo);
    metaStore.insertBackUpInfo(new BackUpInfo(ruleInfo.getId(), "/src/", "/dest/", 100));
    metaStore.insertNewRule(
        new RuleInfo(1, submitTime, rule + "allssd", RuleState.ACTIVE, 0, 0, 0));
    metaStore.insertNewRule(
        new RuleInfo(2, submitTime, rule + "archive", RuleState.ACTIVE, 0, 0, 0));
    metaStore.insertNewRule(
        new RuleInfo(2, submitTime, rule + "onessd", RuleState.ACTIVE, 0, 0, 0));
    metaStore.insertNewRule(new RuleInfo(2, submitTime, rule + "cache", RuleState.ACTIVE, 0, 0, 0));
    Assert.assertTrue(metaStore.listMoveRules().size() == 3);
    Assert.assertTrue(metaStore.listSyncRules().size() == 1);
    CmdletInfo cmdletInfo =
        new CmdletInfo(1, ruleInfo.getId(), CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    cmdletInfo.setAids(Collections.singletonList(1L));
    metaStore.insertCmdlet(cmdletInfo);
    metaStore.insertAction(
        new ActionInfo(1, 1, "allssd", args, "Test", "Test", true, 123213213L, true, 123123L, 100));
    Assert.assertTrue(metaStore.listFileActions(ruleInfo.getId(), 0).size() >= 0);
  }

  @Test
  public void testUpdateCachedFiles() throws Exception {
    metaStore.insertCachedFiles(80L, "testPath", 1000L, 2000L, 100);
    metaStore.insertCachedFiles(90L, "testPath2", 2000L, 3000L, 200);
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
    metaStore.insertCachedFiles(80L, "testPath", 123456L, 234567L, 456);
    Assert.assertTrue(metaStore.getCachedFileStatus(80L).getFromTime() == 123456L);
    // Update record with 80l id
    Assert.assertTrue(metaStore.updateCachedFiles(80L, 234568L, 460));
    Assert.assertTrue(metaStore.getCachedFileStatus().get(0).getLastAccessTime() == 234568L);
    List<CachedFileStatus> list = new LinkedList<>();
    list.add(new CachedFileStatus(321L, "testPath", 113334L, 222222L, 222));
    metaStore.insertCachedFiles(list);
    Assert.assertTrue(metaStore.getCachedFileStatus(321L).getNumAccessed() == 222);
    Assert.assertTrue(metaStore.getCachedFileStatus().size() == 2);
    // Delete one record
    metaStore.deleteCachedFile(321L);
    Assert.assertTrue(metaStore.getCachedFileStatus().size() == 1);
    // Clear all records
    metaStore.deleteAllCachedFile();
    Assert.assertTrue(metaStore.getCachedFileStatus().size() == 0);
    metaStore.insertCachedFiles(80L, "testPath", 123456L, 234567L, 456);
  }

  @Test
  public void testGetCachedFileStatus() throws Exception {
    metaStore.insertCachedFiles(6L, "testPath", 1490918400000L, 234567L, 456);
    metaStore.insertCachedFiles(19L, "testPath", 1490918400000L, 234567L, 456);
    metaStore.insertCachedFiles(23L, "testPath", 1490918400000L, 234567L, 456);
    CachedFileStatus cachedFileStatus = metaStore.getCachedFileStatus(6);
    Assert.assertTrue(cachedFileStatus.getFromTime() == 1490918400000L);
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
    FileInfo[] files = {
      new FileInfo(
          pathString,
          fileId,
          length,
          isDir,
          (short) blockReplication,
          blockSize,
          modTime,
          accessTime,
          (short) 1,
          owner,
          group,
          storagePolicy)
    };
    metaStore.insertFiles(files);
    FileInfo dbFileInfo = metaStore.getFile("/tmp/testFile");
    Assert.assertTrue(dbFileInfo.equals(files[0]));
  }

  @Test
  public void testInsertCmdletsTable() throws Exception {
    CmdletInfo command1 =
        new CmdletInfo(0, 1, CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    metaStore.insertCmdlet(command1);
    CmdletInfo command2 = new CmdletInfo(1, 78, CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    metaStore.insertCmdlet(command2);
    Assert.assertTrue(metaStore.getCmdletById(command1.getCid()).equals(command1));
    Assert.assertTrue(metaStore.getCmdletById(command2.getCid()).equals(command2));
    metaStore.updateCmdlet(command1.getCid(), "TestParameter", CmdletState.DRYRUN);
    Assert.assertTrue(
        metaStore.getCmdletById(command1.getCid()).getParameters().equals("TestParameter"));
    Assert.assertTrue(
        metaStore.getCmdletById(command1.getCid()).getState().equals(CmdletState.DRYRUN));
  }

  @Test
  public void testdeleteFinishedCmdletsWithGenTimeBefore() throws Exception {
    Map<String, String> args = new HashMap();
    CmdletInfo command1 =
        new CmdletInfo(0, 78, CmdletState.CANCELLED, "test", 123L, 232444444L);
    metaStore.insertCmdlet(command1);
    CmdletInfo command2 = new CmdletInfo(1, 78, CmdletState.DONE, "tt", 128L, 232444994L);
    metaStore.insertCmdlet(command2);
    ActionInfo actionInfo =
        new ActionInfo(1, 0, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertAction(actionInfo);
    ActionInfo actionInfo2 =
        new ActionInfo(2, 1, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertAction(actionInfo2);
    ActionInfo actionInfo3 =
        new ActionInfo(3, 0, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertAction(actionInfo3);
    metaStore.deleteFinishedCmdletsWithGenTimeBefore(125);
    Assert.assertTrue(metaStore.getCmdletById(0) == null);
    Assert.assertTrue(metaStore.getActionById(1) == null);
    Assert.assertTrue(metaStore.getActionById(2) != null);
  }

  @Test
  public void testdeleteKeepNewCmdlets() throws Exception {
    Map<String, String> args = new HashMap();
    CmdletInfo command1 =
        new CmdletInfo(0, 78, CmdletState.CANCELLED, "test", 123L, 232444444L);
    metaStore.insertCmdlet(command1);
    CmdletInfo command2 = new CmdletInfo(1, 78, CmdletState.DONE, "tt", 128L, 232444994L);
    metaStore.insertCmdlet(command2);
    ActionInfo actionInfo =
        new ActionInfo(1, 0, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertAction(actionInfo);
    ActionInfo actionInfo2 =
        new ActionInfo(2, 1, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertAction(actionInfo2);
    ActionInfo actionInfo3 =
        new ActionInfo(3, 0, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertAction(actionInfo3);
    metaStore.deleteKeepNewCmdlets(1);
    Assert.assertTrue(metaStore.getCmdletById(0) == null);
    Assert.assertTrue(metaStore.getActionById(1) == null);
    Assert.assertTrue(metaStore.getActionById(2) != null);
  }

  @Test
  public void testUpdateDeleteCommand() throws Exception {
    long commandId = 0;
    commandId = metaStore.getMaxCmdletId();
    System.out.printf("CommandID = %d\n", commandId);
    CmdletInfo command1 = new CmdletInfo(0, 1, CmdletState.PENDING, "test", 123123333L, 232444444L);
    CmdletInfo command2 = new CmdletInfo(1, 78, CmdletState.PENDING, "tt", 123178333L, 232444994L);
    CmdletInfo[] commands = {command1, command2};
    metaStore.insertCmdlets(commands);
    String cidCondition = ">= 1 ";
    String ridCondition = "= 78 ";
    List<CmdletInfo> com = metaStore.getCmdlets(cidCondition, ridCondition, CmdletState.PENDING);
    commandId = metaStore.getMaxCmdletId();
    Assert.assertTrue(commandId == commands.length);
    for (CmdletInfo cmd : com) {
      // System.out.printf("Cid = %d \n", cmd.getCid());
      metaStore.updateCmdlet(cmd.getCid(), cmd.getRid(), CmdletState.DONE);
    }
    List<CmdletInfo> com1 = metaStore.getCmdlets(cidCondition, ridCondition, CmdletState.DONE);
    Assert.assertTrue(com1.size() == 1);

    Assert.assertTrue(com1.get(0).getState().equals(CmdletState.DONE));
    metaStore.deleteCmdlet(command2.getCid());
    com1 = metaStore.getCmdlets(cidCondition, ridCondition, CmdletState.DONE);
    Assert.assertTrue(com1.size() == 0);
  }

  @Test
  public void testInsertListActions() throws Exception {
    Map<String, String> args = new HashMap();
    ActionInfo actionInfo =
        new ActionInfo(1, 1, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertActions(new ActionInfo[] {actionInfo});
    List<ActionInfo> actionInfos = metaStore.getActions(null, null);
    Assert.assertTrue(actionInfos.size() == 1);
    actionInfo.setResult("Finished");
    metaStore.updateActions(new ActionInfo[] {actionInfo});
    actionInfos = metaStore.getActions(null, null);
    Assert.assertTrue(actionInfos.get(0).equals(actionInfo));
  }

  @Test
  public void testGetNewCreatedActions() throws Exception {
    Map<String, String> args = new HashMap();
    List<ActionInfo> actionInfos;
    ActionInfo actionInfo =
        new ActionInfo(1, 1, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertAction(actionInfo);
    actionInfo.setActionId(2);
    metaStore.insertAction(actionInfo);
    actionInfos = metaStore.getNewCreatedActions(1);
    Assert.assertTrue(actionInfos.size() == 1);
    actionInfos = metaStore.getNewCreatedActions("cache", 1, true, true);
    Assert.assertTrue(actionInfos.size() == 1);
    actionInfos = metaStore.getNewCreatedActions(2);
    Assert.assertTrue(actionInfos.size() == 2);
  }

  @Test
  public void testGetMaxActionId() throws Exception {
    long currentId = metaStore.getMaxActionId();
    Map<String, String> args = new HashMap();
    Assert.assertTrue(currentId == 0);
    ActionInfo actionInfo =
        new ActionInfo(
            currentId, 1, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertActions(new ActionInfo[] {actionInfo});
    currentId = metaStore.getMaxActionId();
    Assert.assertTrue(currentId == 1);
    actionInfo =
        new ActionInfo(
            currentId, 1, "cache", args, "Test", "Test", true, 123213213L, true, 123123L, 100);
    metaStore.insertActions(new ActionInfo[] {actionInfo});
    currentId = metaStore.getMaxActionId();
    Assert.assertTrue(currentId == 2);
  }

  @Test
  public void testInsertStoragePolicyTable() throws Exception {
    metaStore.insertStoragePolicy(new StoragePolicy((byte) 53, "COOL"));
    metaStore.insertStoragePolicy(new StoragePolicy((byte) 52, "COLD"));
    String value = metaStore.getStoragePolicyName(53);
    Assert.assertEquals(metaStore.getStoragePolicyName(52), "COLD");
    int key = metaStore.getStoragePolicyID("COOL");
    Assert.assertEquals(value, "COOL");
    Assert.assertEquals(key, 53);
  }

  @Test
  public void testInsertXattrTable() throws Exception {
    long fid = 567L;
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
    ClusterConfig clusterConfig = new ClusterConfig(1, "test", "test1");
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
    GlobalConfig globalConfig = new GlobalConfig(1, "test", "test1");
    metaStore.setGlobalConfig(globalConfig);
    Assert.assertTrue(metaStore.getDefaultGlobalConfigByName("test").equals(globalConfig));
    globalConfig.setPropertyValue("test2");

    metaStore.setGlobalConfig(globalConfig);
    Assert.assertTrue(metaStore.getDefaultGlobalConfigByName("test").equals(globalConfig));
  }

  @Test
  public void testInsertDataNodeInfo() throws Exception {
    DataNodeInfo insertInfo1 = new DataNodeInfo("UUID1", "hostname", "www.ssm.com", 100, 50, "lab");
    metaStore.insertDataNodeInfo(insertInfo1);
    List<DataNodeInfo> getInfo1 = metaStore.getDataNodeInfoByUuid("UUID1");
    Assert.assertTrue(insertInfo1.equals(getInfo1.get(0)));

    DataNodeInfo insertInfo2 = new DataNodeInfo("UUID2", "HOSTNAME", "www.ssm.com", 0, 0, null);
    DataNodeInfo insertInfo3 = new DataNodeInfo("UUID3", "HOSTNAME", "www.ssm.com", 0, 0, null);
    metaStore.insertDataNodeInfos(new DataNodeInfo[] {insertInfo2, insertInfo3});
    List<DataNodeInfo> getInfo2 = metaStore.getDataNodeInfoByUuid("UUID2");
    Assert.assertTrue(insertInfo2.equals(getInfo2.get(0)));
    List<DataNodeInfo> getInfo3 = metaStore.getDataNodeInfoByUuid("UUID3");
    Assert.assertTrue(insertInfo3.equals(getInfo3.get(0)));
  }

  @Test
  public void testDeleteDataNodeInfo() throws Exception {
    DataNodeInfo insertInfo1 = new DataNodeInfo("UUID1", "hostname", "www.ssm.com", 100, 50, "lab");
    DataNodeInfo insertInfo2 = new DataNodeInfo("UUID2", "HOSTNAME", "www.ssm.com", 0, 0, null);
    DataNodeInfo insertInfo3 = new DataNodeInfo("UUID3", "HOSTNAME", "www.ssm.com", 0, 0, null);
    metaStore.insertDataNodeInfos(new DataNodeInfo[] {insertInfo1, insertInfo2, insertInfo3});

    List<DataNodeInfo> infos = metaStore.getAllDataNodeInfo();
    Assert.assertTrue(infos.size() == 3);

    metaStore.deleteDataNodeInfo(insertInfo1.getUuid());
    infos = metaStore.getAllDataNodeInfo();
    Assert.assertTrue(infos.size() == 2);

    metaStore.deleteAllDataNodeInfo();
    infos = metaStore.getAllDataNodeInfo();
    Assert.assertTrue(infos.size() == 0);
  }

  @Test
  public void testInsertDataNodeStorageInfo() throws Exception {
    DataNodeStorageInfo insertInfo1 =
        new DataNodeStorageInfo("UUID1", 10, 10, "storageid1", 0, 0, 0, 0, 0);
    metaStore.insertDataNodeStorageInfo(insertInfo1);
    List<DataNodeStorageInfo> getInfo1 = metaStore.getDataNodeStorageInfoByUuid("UUID1");
    Assert.assertTrue(insertInfo1.equals(getInfo1.get(0)));

    DataNodeStorageInfo insertInfo2 =
        new DataNodeStorageInfo("UUID2", 10, 10, "storageid2", 0, 0, 0, 0, 0);
    DataNodeStorageInfo insertInfo3 =
        new DataNodeStorageInfo("UUID3", 10, 10, "storageid2", 0, 0, 0, 0, 0);
    metaStore.insertDataNodeStorageInfos(new DataNodeStorageInfo[] {insertInfo2, insertInfo3});
    List<DataNodeStorageInfo> getInfo2 = metaStore.getDataNodeStorageInfoByUuid("UUID2");
    Assert.assertTrue(insertInfo2.equals(getInfo2.get(0)));
    List<DataNodeStorageInfo> getInfo3 = metaStore.getDataNodeStorageInfoByUuid("UUID3");
    Assert.assertTrue(insertInfo3.equals(getInfo3.get(0)));
  }

  @Test
  public void testDeleteDataNodeStorageInfo() throws Exception {
    DataNodeStorageInfo insertInfo1 =
        new DataNodeStorageInfo("UUID1", 10, 10, "storageid1", 0, 0, 0, 0, 0);
    DataNodeStorageInfo insertInfo2 =
        new DataNodeStorageInfo("UUID2", 10, 10, "storageid2", 0, 0, 0, 0, 0);
    DataNodeStorageInfo insertInfo3 =
        new DataNodeStorageInfo("UUID3", 10, 10, "storageid3", 0, 0, 0, 0, 0);
    metaStore.insertDataNodeStorageInfos(
        new DataNodeStorageInfo[] {insertInfo1, insertInfo2, insertInfo3});

    List<DataNodeStorageInfo> infos = metaStore.getAllDataNodeStorageInfo();
    Assert.assertTrue(infos.size() == 3);

    metaStore.deleteDataNodeStorageInfo(insertInfo1.getUuid());
    infos = metaStore.getAllDataNodeStorageInfo();
    Assert.assertTrue(infos.size() == 2);

    metaStore.deleteAllDataNodeStorageInfo();
    infos = metaStore.getAllDataNodeStorageInfo();
    Assert.assertTrue(infos.size() == 0);
  }

  @Test
  public void testInsertAndListAllBackUpInfo() throws MetaStoreException {
    BackUpInfo backUpInfo1 = new BackUpInfo(1, "test1", "test1", 1);
    BackUpInfo backUpInfo2 = new BackUpInfo(2, "test2", "test2", 2);
    BackUpInfo backUpInfo3 = new BackUpInfo(3, "test3", "test3", 3);

    metaStore.insertBackUpInfo(backUpInfo1);
    metaStore.insertBackUpInfo(backUpInfo2);
    metaStore.insertBackUpInfo(backUpInfo3);

    List<BackUpInfo> backUpInfos = metaStore.listAllBackUpInfo();

    Assert.assertTrue(backUpInfos.get(0).equals(backUpInfo1));
    Assert.assertTrue(backUpInfos.get(1).equals(backUpInfo2));
    Assert.assertTrue(backUpInfos.get(2).equals(backUpInfo3));
  }

  @Test
  public void testGetBackUpInfoById() throws MetaStoreException {
    BackUpInfo backUpInfo1 = new BackUpInfo(1, "test1", "test1", 1);
    metaStore.insertBackUpInfo(backUpInfo1);
    Assert.assertTrue(metaStore.getBackUpInfo(1).equals(backUpInfo1));
  }

  @Test
  public void testDeleteBackUpInfo() throws MetaStoreException {
    BackUpInfo backUpInfo1 = new BackUpInfo(1, "test1", "test1", 1);
    metaStore.insertBackUpInfo(backUpInfo1);
    Assert.assertTrue(metaStore.srcInbackup("test1/dfafdsaf"));
    Assert.assertFalse(metaStore.srcInbackup("test2"));
    metaStore.deleteBackUpInfo(1);

    Assert.assertTrue(metaStore.listAllBackUpInfo().size() == 0);

    metaStore.insertBackUpInfo(backUpInfo1);
    metaStore.deleteAllBackUpInfo();

    Assert.assertTrue(metaStore.listAllBackUpInfo().size() == 0);
  }

  @Test
  public void testInsertAndListAllClusterInfo() throws MetaStoreException {
    ClusterInfo clusterInfo1 = new ClusterInfo(1, "test1", "test1", "test1", "test1", "test1");
    ClusterInfo clusterInfo2 = new ClusterInfo(2, "test2", "test2", "test2", "test2", "test2");

    metaStore.insertClusterInfo(clusterInfo1);
    metaStore.insertClusterInfo(clusterInfo2);

    List<ClusterInfo> clusterInfos = metaStore.listAllClusterInfo();

    Assert.assertTrue(clusterInfos.get(0).equals(clusterInfo1));
    Assert.assertTrue(clusterInfos.get(1).equals(clusterInfo2));
  }

  @Test
  public void testGetClusterInfoById() throws MetaStoreException {
    ClusterInfo clusterInfo = new ClusterInfo(1, "test1", "test1", "test1", "test1", "test1");
    metaStore.insertClusterInfo(clusterInfo);

    Assert.assertTrue(metaStore.getClusterInfoByCid(1).equals(clusterInfo));
  }

  @Test
  public void testDelectBackUpInfo() throws MetaStoreException {
    ClusterInfo clusterInfo = new ClusterInfo(1, "test1", "test1", "test1", "test1", "test1");
    metaStore.insertClusterInfo(clusterInfo);

    metaStore.deleteClusterInfo(1);

    Assert.assertTrue(metaStore.listAllClusterInfo().size() == 0);

    metaStore.insertClusterInfo(clusterInfo);
    metaStore.deleteAllClusterInfo();

    Assert.assertTrue(metaStore.listAllClusterInfo().size() == 0);
  }

  @Test
  public void testInsertSystemInfo() throws MetaStoreException {
    SystemInfo systemInfo = new SystemInfo("test", "test");
    metaStore.insertSystemInfo(systemInfo);
    Assert.assertTrue(metaStore.getSystemInfoByProperty("test").equals(systemInfo));
  }

  @Test
  public void testDeleteSystemInfo() throws MetaStoreException {
    SystemInfo systemInfo = new SystemInfo("test", "test");
    metaStore.insertSystemInfo(systemInfo);
    metaStore.deleteSystemInfo("test");

    Assert.assertTrue(metaStore.listAllSystemInfo().size() == 0);
  }

  @Test
  public void testUpdateSystemInfo() throws MetaStoreException {
    SystemInfo systemInfo = new SystemInfo("test", "test");
    metaStore.insertSystemInfo(systemInfo);
    SystemInfo newSystemInfo = new SystemInfo("test", "test1");
    metaStore.updateSystemInfo(newSystemInfo);
    Assert.assertTrue(metaStore.getSystemInfoByProperty("test").equals(newSystemInfo));
  }

  @Test
  public void testUpdateAndInsertSystemInfo() throws MetaStoreException {
    SystemInfo systemInfo = new SystemInfo("test", "test");
    metaStore.updateAndInsertIfNotExist(systemInfo);
    Assert.assertTrue(metaStore.containSystemInfo("test"));
    Assert.assertTrue(metaStore.getSystemInfoByProperty("test").equals(systemInfo));
  }
}
