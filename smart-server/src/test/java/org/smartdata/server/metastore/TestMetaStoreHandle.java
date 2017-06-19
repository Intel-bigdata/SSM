package org.smartdata.server.metastore;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.hdfs.CacheFileAction;
import org.smartdata.common.CmdletState;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.cmdlet.CmdletInfo;
import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.metrics.FileAccessEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class TestMetaStoreHandle extends TestDaoUtil {
  private MetaStoreHandle metaStoreHandle;

  private void reInit() throws Exception {
    // Clear DB and create new tables
    metaStoreHandle = new MetaStoreHandle(druidPool);
  }

/*  @Test
  public void testGetAccessCount() throws Exception {
    init();
    Map<Long, Integer> ret = metaStoreHandle.getAccessCount(1490932740000l,
        1490936400000l, null);
    Assert.assertTrue(ret.get(2l) == 32);
  }
*/

  @Test
  public void testGetFiles() throws Exception {
    reInit();
    String pathString = "des";
    long length = 20484l;
    boolean isDir = false;
    int blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 1490936390000l;
    FsPermission perms = FsPermission.getDefault();
    String owner = "root";
    String group = "admin";
    byte[] symlink = null;
    byte[] path = DFSUtil.string2Bytes(pathString);
    long fileId = 56l;
    int numChildren = 0;
    byte storagePolicy = 0;
    FileStatusInternal[] files = {new FileStatusInternal(length, isDir, blockReplication,
        blockSize, modTime, accessTime, perms, owner, group, symlink,
        path, "/tmp", fileId, numChildren, null, storagePolicy)};
    metaStoreHandle.insertFiles(files);
    HdfsFileStatus hdfsFileStatus = metaStoreHandle.getFile(56);
    Assert.assertTrue(hdfsFileStatus.getLen() == 20484l);
    hdfsFileStatus = metaStoreHandle.getFile("/tmp/des");
    Assert.assertTrue(hdfsFileStatus.getAccessTime() == 1490936390000l);
  }

  @Test
  public void testInsertStoragesTable() throws Exception {
    reInit();
    StorageCapacity storage1 = new StorageCapacity("Flash",
        12343333l, 2223333l);
    StorageCapacity storage2 = new StorageCapacity("RAM",
        12342233l, 2223663l);
    StorageCapacity[] storages = {storage1, storage2};
    metaStoreHandle.insertStoragesTable(storages);
    StorageCapacity storageCapacity1 = metaStoreHandle
        .getStorageCapacity("Flash");
    StorageCapacity storageCapacity2 = metaStoreHandle
        .getStorageCapacity("RAM");
    Assert.assertTrue(storageCapacity1.getCapacity() == 12343333l);
    Assert.assertTrue(storageCapacity2.getFree() == 2223663l);
    Assert.assertTrue(metaStoreHandle.updateStoragesTable("Flash",
        123456L, 4562233L));
    Assert.assertTrue(metaStoreHandle.getStorageCapacity("Flash")
        .getCapacity() == 12343333l);
  }


  @Test
  public void testGetStorageCapacity() throws Exception {
    reInit();
    StorageCapacity storage1 = new StorageCapacity("HDD",
        12343333l, 2223333l);
    StorageCapacity storage2 = new StorageCapacity("RAM",
        12342233l, 2223663l);
    StorageCapacity[] storages = {storage1, storage2};
    metaStoreHandle.insertStoragesTable(storages);
    StorageCapacity storageCapacity = metaStoreHandle.getStorageCapacity("HDD");
    Assert.assertTrue(storageCapacity.getCapacity() == 12343333l);
  }

  @Test
  public void testUpdateCachedFiles() throws Exception {
    reInit();
    metaStoreHandle.insertCachedFiles(80L, "testPath", 1000L,
        2000L, 100);
    metaStoreHandle.insertCachedFiles(90L, "testPath2", 2000L,
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

    metaStoreHandle.updateCachedFiles(pathToId, events);
    List<CachedFileStatus> statuses = metaStoreHandle.getCachedFileStatus();
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
    reInit();
    metaStoreHandle.insertCachedFiles(80l, "testPath", 123456l,
        234567l, 456);
    Assert.assertTrue(metaStoreHandle.getCachedFileStatus(
        80l).getFromTime() == 123456l);
    // Update record with 80l id
    Assert.assertTrue(metaStoreHandle.updateCachedFiles(80l, 123455l,
        234568l, 460));
    Assert.assertTrue(metaStoreHandle.getCachedFileStatus().get(0)
        .getLastAccessTime() == 234568l);
    List<CachedFileStatus> list = new LinkedList<>();
    list.add(new CachedFileStatus(321l, "testPath", 113334l,
        222222l, 222));
    metaStoreHandle.insertCachedFiles(list);
    Assert.assertTrue(metaStoreHandle.getCachedFileStatus(321l)
        .getNumAccessed() == 222);
    Assert.assertTrue(metaStoreHandle.getCachedFileStatus().size() == 2);
    // Delete one record
    metaStoreHandle.deleteCachedFile(321l);
    Assert.assertTrue(metaStoreHandle.getCachedFileStatus().size() == 1);
    // Clear all records
    metaStoreHandle.deleteAllCachedFile();
    Assert.assertTrue(metaStoreHandle.getCachedFileStatus().size() == 0);
    metaStoreHandle.insertCachedFiles(80l, "testPath", 123456l,
        234567l, 456);
  }

  @Test
  public void testGetCachedFileStatus() throws Exception {
    reInit();
    metaStoreHandle.insertCachedFiles(6l, "testPath", 1490918400000l,
        234567l, 456);
    metaStoreHandle.insertCachedFiles(19l, "testPath", 1490918400000l,
        234567l, 456);
    metaStoreHandle.insertCachedFiles(23l, "testPath", 1490918400000l,
        234567l, 456);
    CachedFileStatus cachedFileStatus = metaStoreHandle.getCachedFileStatus(6);
    Assert.assertTrue(cachedFileStatus.getFromTime() == 1490918400000l);
    List<CachedFileStatus> cachedFileList = metaStoreHandle.getCachedFileStatus();
    List<Long> fids = metaStoreHandle.getCachedFids();
    Assert.assertTrue(fids.size() == 3);
    Assert.assertTrue(cachedFileList.get(0).getFid() == 6);
    Assert.assertTrue(cachedFileList.get(1).getFid() == 19);
    Assert.assertTrue(cachedFileList.get(2).getFid() == 23);
  }

  @Test
  public void testInsetFiles() throws Exception {
    reInit();
    String pathString = "testFile";
    long length = 123L;
    boolean isDir = false;
    int blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    FsPermission perms = FsPermission.getDefault();
    String owner = "root";
    String group = "admin";
    byte[] symlink = null;
    byte[] path = DFSUtil.string2Bytes(pathString);
    long fileId = 312321L;
    int numChildren = 0;
    byte storagePolicy = 0;
    FileStatusInternal[] files = {new FileStatusInternal(length, isDir, blockReplication,
        blockSize, modTime, accessTime, perms, owner, group, symlink,
        path, "/tmp", fileId, numChildren, null, storagePolicy)};
    metaStoreHandle.insertFiles(files);
    HdfsFileStatus hdfsFileStatus = metaStoreHandle.getFile("/tmp/testFile");
    Assert.assertTrue(hdfsFileStatus.getBlockSize() == 128 * 1024L);
  }

  @Test
  public void testInsertCommandsTable() throws Exception {
    reInit();
    CmdletInfo command1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333l, 232444444l);
    CmdletInfo command2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333l, 232444994l);
    CmdletInfo[] commands = {command1, command2};
    metaStoreHandle.insertCmdletsTable(commands);
    String cidCondition = ">= 0 ";
    String ridCondition = "= 78 ";
    CmdletState state = null;
    CmdletState state1 = CmdletState.PAUSED;
    List<CmdletInfo> com = metaStoreHandle.getCmdletsTableItem(cidCondition, ridCondition, state);
    Assert.assertTrue(com.get(0).getState() == CmdletState.PAUSED);
    List<CmdletInfo> com1 = metaStoreHandle.getCmdletsTableItem(null,
        null, state1);
    Assert.assertTrue(com1.get(0).getState() == CmdletState.PAUSED);
  }

  @Test
  public void testUpdateCommand() throws Exception {
    reInit();
    long commandId = 0;
    commandId = metaStoreHandle.getMaxCmdletId();
    System.out.printf("CommandID = %d\n", commandId);
    CmdletInfo command1 = new CmdletInfo(0, 1,
        CmdletState.PENDING, "test", 123123333l, 232444444l);
    CmdletInfo command2 = new CmdletInfo(1, 78,
        CmdletState.PENDING, "tt", 123178333l, 232444994l);
    CmdletInfo[] commands = {command1, command2};
    metaStoreHandle.insertCmdletsTable(commands);
    commandId = metaStoreHandle.getMaxCmdletId();
    System.out.printf("CommandID = %d\n", commandId);
    String cidCondition = ">= 1 ";
    String ridCondition = "= 78 ";
    List<CmdletInfo> com = metaStoreHandle
        .getCmdletsTableItem(cidCondition, ridCondition, CmdletState.PENDING);
    commandId = metaStoreHandle.getMaxCmdletId();
    Assert.assertTrue(commandId == commands.length);
    for (CmdletInfo cmd : com) {
      // System.out.printf("Cid = %d \n", cmd.getCid());
      metaStoreHandle.updateCmdletStatus(cmd.getCid(), cmd.getRid(), CmdletState.DONE);
    }
    List<CmdletInfo> com1 = metaStoreHandle.getCmdletsTableItem(cidCondition, ridCondition, CmdletState.DONE);
    Assert.assertTrue(com1.size() == 1);
    Assert.assertTrue(com1.get(0).getState() == CmdletState.DONE);
  }

  @Test
  public void testInsertListActions() throws Exception {
    reInit();
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, "/test/file");
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStoreHandle.insertActionsTable(new ActionInfo[]{actionInfo});
    List<ActionInfo> actionInfos = metaStoreHandle.getActionsTableItem(null, null);
    Assert.assertTrue(actionInfos.size() == 1);
  }

  @Test
  public void testGetNewCreatedActions() throws Exception {
    reInit();
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, "/test/file");
    List<ActionInfo> actionInfos;
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStoreHandle.insertActionTable(actionInfo);
    actionInfo.setActionId(2);
    metaStoreHandle.insertActionTable(actionInfo);
    actionInfos = metaStoreHandle.getNewCreatedActionsTableItem(1);
    Assert.assertTrue(actionInfos.size() == 1);
    actionInfos = metaStoreHandle.getNewCreatedActionsTableItem(2);
    Assert.assertTrue(actionInfos.size() == 2);
  }

  @Test
  public void testGetMaxActionId() throws Exception {
    reInit();
    long currentId = metaStoreHandle.getMaxActionId();
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, "/test/file");
    System.out.printf("ActionID = %d\n", currentId);
    Assert.assertTrue(currentId == 0);
    ActionInfo actionInfo = new ActionInfo(currentId, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStoreHandle.insertActionsTable(new ActionInfo[]{actionInfo});
    currentId = metaStoreHandle.getMaxActionId();
    System.out.printf("ActionID = %d\n", currentId);
    Assert.assertTrue(currentId == 1);
    actionInfo = new ActionInfo(currentId, 1,
        "cache", args, "Test",
        "Test", true, 123213213l, true, 123123l,
        100);
    metaStoreHandle.insertActionsTable(new ActionInfo[]{actionInfo});
    currentId = metaStoreHandle.getMaxActionId();
    System.out.printf("ActionID = %d\n", currentId);
    Assert.assertTrue(currentId == 2);
  }

  @Test
  public void testInsertStoragePolicyTable() throws Exception {
    reInit();
    StoragePolicy s = new StoragePolicy((byte) 3, "COOL");
    Assert.assertEquals(metaStoreHandle.getStoragePolicyName(2), "COLD");
    metaStoreHandle.insertStoragePolicyTable(s);
    String value = metaStoreHandle.getStoragePolicyName(3);
    int key = metaStoreHandle.getStoragePolicyID("COOL");
    Assert.assertEquals(value, "COOL");
    Assert.assertEquals(key, 3);
  }

  @Test
  public void testInsertXattrTable() throws Exception {
    reInit();
    long fid = 567l;
    Map<String, byte[]> xAttrMap = new HashMap<>();
    String name1 = "user.a1";
    String name2 = "raw.you";
    Random random = new Random();
    byte[] value1 = new byte[1024];
    byte[] value2 = new byte[1024];
    random.nextBytes(value1);
    random.nextBytes(value2);
    xAttrMap.put(name1, value1);
    xAttrMap.put(name2, value2);
    Assert.assertTrue(metaStoreHandle.insertXattrTable(fid, xAttrMap));
    Map<String, byte[]> map = metaStoreHandle.getXattrTable(fid);
    Assert.assertTrue(map.size() == xAttrMap.size());
    for (String m : map.keySet()) {
      Assert.assertArrayEquals(map.get(m), xAttrMap.get(m));
    }
  }
}
