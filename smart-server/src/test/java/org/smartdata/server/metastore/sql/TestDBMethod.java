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
package org.smartdata.server.metastore;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.common.CommandState;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.common.actions.ActionType;
import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.server.metastore.*;

import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestDBMethod {
    @Test
    public void testGetAccessCount() throws Exception {
      Connection conn = null;
      try {
        conn = new TestDBUtil().getTestDBInstance();
        DBAdapter dbAdapter = new DBAdapter(conn);
        Map<Long, Integer> ret = dbAdapter.getAccessCount(1490932740000l,
            1490936400000l, null);
        Assert.assertTrue(ret.get(2l) == 32);
      } finally {
        if (conn != null) {
          conn.close();
        }
      }
    }

    @Test
    public void testGetFiles() throws Exception {
      Connection conn = null;
      try {
        conn = new TestDBUtil().getTestDBInstance();
        DBAdapter dbAdapter = new DBAdapter(conn);
        HdfsFileStatus hdfsFileStatus = dbAdapter.getFile(56);
        Assert.assertTrue(hdfsFileStatus.getLen() == 20484l);
        HdfsFileStatus hdfsFileStatus1 = dbAdapter.getFile("/des");
        Assert.assertTrue(hdfsFileStatus1.getAccessTime() == 1490936390000l);
      } finally {
        if (conn != null) {
          conn.close();
        }
      }
    }

    @Test
    public void testInsertStoragesTable() throws Exception {
      String dbFile = TestDBUtil.getUniqueDBFilePath();
      Connection conn = null;
      try {
        conn = Util.createSqliteConnection(dbFile);
        Util.initializeDataBase(conn);
        DBAdapter dbAdapter = new DBAdapter(conn);
        StorageCapacity storage1 = new StorageCapacity("Flash",
            12343333l, 2223333l);
        StorageCapacity storage2 = new StorageCapacity("RAM",
            12342233l, 2223663l);
        StorageCapacity[] storages = {storage1, storage2};
        dbAdapter.insertStoragesTable(storages);
        StorageCapacity storageCapacity1 = dbAdapter
            .getStorageCapacity("Flash");
        StorageCapacity storageCapacity2 = dbAdapter
            .getStorageCapacity("RAM");
        Assert.assertTrue(storageCapacity1.getCapacity() == 12343333l);
        Assert.assertTrue(storageCapacity2.getFree() == 2223663l);
        Assert.assertTrue(dbAdapter.updateStoragesTable("Flash",
            123456L, 4562233L));
        Assert.assertTrue(dbAdapter.getStorageCapacity("Flash")
            .getCapacity() == 123456l);
      } finally {
        if (conn != null) {
          conn.close();
        }
        File file = new File(dbFile);
        file.deleteOnExit();
      }
    }

    @Test
    public void testGetStorageCapacity() throws Exception {
      Connection conn = null;
      try {
        conn = new TestDBUtil().getTestDBInstance();
        DBAdapter dbAdapter = new DBAdapter(conn);
        StorageCapacity storageCapacity = dbAdapter.getStorageCapacity("HDD");
        Assert.assertTrue(storageCapacity.getCapacity() == 65536000l);
      } finally {
        if (conn != null) {
          conn.close();
        }
      }
    }

    @Test
    public void testInsertCachedFiles() throws Exception {
      String dbFile = TestDBUtil.getUniqueDBFilePath();
      Connection conn = null;
      try {
        conn = Util.createSqliteConnection(dbFile);
        Util.initializeDataBase(conn);
        DBAdapter dbAdapter = new DBAdapter(conn);
        dbAdapter.insertCachedFiles(80l, 123456l,
            234567l, 456);
        Assert.assertTrue(dbAdapter.getCachedFileStatus(
            80l).getFromTime() == 123456l);
        Assert.assertTrue(dbAdapter.updateCachedFiles(80l, 123455l,
            234568l, 460));
        Assert.assertTrue(dbAdapter.getCachedFileStatus().get(0)
            .getLastAccessTime() == 234568l);
        List<CachedFileStatus> list = new LinkedList<>();
        list.add(new CachedFileStatus(321l, 113334l,
            222222l, 222));
        dbAdapter.insertCachedFiles(list);
        Assert.assertTrue(dbAdapter.getCachedFileStatus(321l)
            .getNumAccessed() == 222);
      } finally {
        if (conn != null) {
          conn.close();
        }
        File file = new File(dbFile);
        file.deleteOnExit();
      }
    }

    @Test
    public void testGetCachedFileStatus() throws Exception {
      Connection conn = null;
      try {
          conn = new TestDBUtil().getTestDBInstance();
        DBAdapter dbAdapter = new DBAdapter(conn);
        CachedFileStatus cachedFileStatus = dbAdapter.getCachedFileStatus(6);
        Assert.assertTrue(cachedFileStatus.getFromTime() == 1490918400000l);
        List<CachedFileStatus> cachedFileList = dbAdapter.getCachedFileStatus();
        Assert.assertTrue(cachedFileList.get(0).getFid() == 6);
        Assert.assertTrue(cachedFileList.get(1).getFid() == 19);
        Assert.assertTrue(cachedFileList.get(2).getFid() == 23);
      } finally {
        if (conn != null) {
          conn.close();
        }
      }
    }

    @Test
    public void testInsetFiles() throws Exception {
      Connection conn = null;
      try {
        conn = new TestDBUtil().getTestDBInstance();
        DBAdapter dbAdapter = new DBAdapter(conn);
        String pathString = "testFile";
        long length = 123L;
        boolean isDir = false;
        int blockReplication = 1;
        long blockSize = 128 *1024L;
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
        FileStatusInternal[] files = { new FileStatusInternal(length, isDir, blockReplication,
            blockSize, modTime, accessTime, perms, owner, group, symlink,
            path, "/tmp", fileId, numChildren, null, storagePolicy) };
        dbAdapter.insertFiles(files);
        HdfsFileStatus hdfsFileStatus = dbAdapter.getFile("/tmp/testFile");
        Assert.assertTrue(hdfsFileStatus.getBlockSize() == 128 * 1024L);
      } finally {
        if (conn != null) {
          conn.close();
        }
      }
    }

    @Test
    public void testInsertCommandsTable() throws Exception {
      String dbFile = TestDBUtil.getUniqueDBFilePath();
      Connection conn = null;
      try {
        conn = Util.createSqliteConnection(dbFile);
        Util.initializeDataBase(conn);
        DBAdapter dbAdapter = new DBAdapter(conn);
        CommandInfo command1 = new CommandInfo(0, 1, ActionType.None,
            CommandState.EXECUTING, "test", 123123333l, 232444444l);
        CommandInfo command2 = new CommandInfo(0, 78, ActionType.ConvertToEC,
            CommandState.PAUSED, "tt", 123178333l, 232444994l);
        CommandInfo[] commands = {command1, command2};
        dbAdapter.insertCommandsTable(commands);
        String cidCondition = ">= 2 ";
        String ridCondition = "= 78 ";
        CommandState state = null;
        CommandState state1 = CommandState.PAUSED;
        List<CommandInfo> com = dbAdapter.getCommandsTableItem(cidCondition, ridCondition, state);
        Assert.assertTrue(com.get(0).getActionType() == ActionType.ConvertToEC);
        Assert.assertTrue(com.get(0).getState() == CommandState.PAUSED);
        List<CommandInfo> com1 = dbAdapter.getCommandsTableItem(null,
            null, state1);
        Assert.assertTrue(com1.get(0).getState() == CommandState.PAUSED);
      } finally {
        if (conn != null) {
          conn.close();
        }
        File file = new File(dbFile);
        file.deleteOnExit();
      }
    }

    @Test
    public void testUpdateCommand() throws Exception {
      String dbFile = TestDBUtil.getUniqueDBFilePath();
      Connection conn = null;
      try {
        conn = Util.createSqliteConnection(dbFile);
        Util.initializeDataBase(conn);
        DBAdapter dbAdapter = new DBAdapter(conn);
        CommandInfo command1 = new CommandInfo(0, 1, ActionType.None,
                CommandState.PENDING, "test", 123123333l, 232444444l);
        CommandInfo command2 = new CommandInfo(0, 78, ActionType.ConvertToEC,
                CommandState.PENDING, "tt", 123178333l, 232444994l);
        CommandInfo[] commands = {command1, command2};
        dbAdapter.insertCommandsTable(commands);
        String cidCondition = ">= 1 ";
        String ridCondition = "= 78 ";
        List<CommandInfo> com = dbAdapter.getCommandsTableItem(cidCondition, ridCondition, CommandState.PENDING);
        for(CommandInfo cmd: com) {
          // System.out.printf("Cid = %d \n", cmd.getCid());
          dbAdapter.updateCommandStatus(cmd.getCid(), cmd.getRid(), CommandState.DONE);
        }
        List<CommandInfo> com1 = dbAdapter.getCommandsTableItem(cidCondition, ridCondition, CommandState.DONE);
        Assert.assertTrue(com1.size() == 1);
        Assert.assertTrue(com1.get(0).getState() == CommandState.DONE);
      } finally {
        if (conn != null) {
          conn.close();
        }
        File file = new File(dbFile);
        file.deleteOnExit();
      }
    }


    @Test
    public void testInsertStoragePolicyTable() throws Exception {
      String dbFile = TestDBUtil.getUniqueDBFilePath();
      Connection conn = null;
      try {
        conn = Util.createSqliteConnection(dbFile);
        Util.initializeDataBase(conn);
        DBAdapter dbAdapter = new DBAdapter(conn);
        StoragePolicy s = new StoragePolicy((byte)3,"COOL");
        Assert.assertEquals(dbAdapter.getStoragePolicyName(2), "COLD");
        dbAdapter.insertStoragePolicyTable(s);
        String value = dbAdapter.getStoragePolicyName(3);
        int key = dbAdapter.getStoragePolicyID("COOL");
        Assert.assertEquals(value, "COOL");
        Assert.assertEquals(key , 3);
      } finally {
        if (conn != null) {
          conn.close();
        }
        File file = new File(dbFile);
        file.deleteOnExit();
      }
    }

  @Test
  public void testInsertXattrTable() throws Exception {
    String dbFile = TestDBUtil.getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = Util.createSqliteConnection(dbFile);
      Util.initializeDataBase(conn);
      DBAdapter dbAdapter = new DBAdapter(conn);
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
      Assert.assertTrue(dbAdapter.insertXattrTable(fid, xAttrMap));
      Map<String, byte[]> map = dbAdapter.getXattrTable(fid);
      Assert.assertTrue(map.size() == xAttrMap.size());
      for (String m : map.keySet()) {
        Assert.assertArrayEquals(map.get(m), xAttrMap.get(m));
      }
    } finally {
      if (conn != null) {
        conn.close();
      }
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }
}
