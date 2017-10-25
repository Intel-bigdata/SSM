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
package org.smartdata.metastore.dao;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.FileInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFileInfoDao extends TestDaoUtil {
  private FileInfoDao fileInfoDao;

  @Before
  public void initFileDao() throws Exception {
    initDao();
    fileInfoDao = new FileInfoDao(druidPool.getDataSource());
  }

  @After
  public void closeFileDao() throws Exception {
    closeDao();
    fileInfoDao = null;
  }

  @Test
  public void testInsetGetDeleteFiles() throws Exception {
    String path = "/testFile";
    long length = 123L;
    boolean isDir = false;
    short blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    short permission = 1;
    String owner = "root";
    String group = "admin";
    long fileId = 312321L;
    byte storagePolicy = 0;
    Map<Integer, String> mapOwnerIdName = new HashMap<>();
    mapOwnerIdName.put(1, "root");
    Map<Integer, String> mapGroupIdName = new HashMap<>();
    mapGroupIdName.put(1, "admin");
    fileInfoDao.updateUsersMap(mapOwnerIdName);
    fileInfoDao.updateGroupsMap(mapGroupIdName);
    FileInfo fileInfo = new FileInfo(path, fileId, length, isDir, blockReplication,
        blockSize, modTime, accessTime, permission, owner, group, storagePolicy);
    fileInfoDao.insert(fileInfo);
    FileInfo file1 = fileInfoDao.getByPath("/testFile");
    Assert.assertTrue(fileInfo.equals(file1));
    FileInfo file2 = fileInfoDao.getById(fileId);
    Assert.assertTrue(fileInfo.equals(file2));
    FileInfo fileInfo1 = new FileInfo(path, fileId + 1, length, isDir, blockReplication,
        blockSize, modTime, accessTime, permission, owner, group, storagePolicy);
    fileInfoDao.insert(fileInfo1);
    List<FileInfo> fileInfos = fileInfoDao.getFilesByPrefix("/testaaFile");
    Assert.assertTrue(fileInfos.size() == 0);
    fileInfos = fileInfoDao.getFilesByPrefix("/testFile");
    Assert.assertTrue(fileInfos.size() == 2);
    fileInfoDao.deleteById(fileId);
    fileInfos = fileInfoDao.getAll();
    Assert.assertTrue(fileInfos.size() == 1);
    fileInfoDao.deleteAll();
    fileInfos = fileInfoDao.getAll();
    Assert.assertTrue(fileInfos.size() == 0);
  }

  @Test
  public void testInsertUpdateFiles() throws Exception {
    String path = "/testFile";
    long length = 123L;
    boolean isDir = false;
    short blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    short permission = 1;
    String owner = "root";
    String group = "admin";
    long fileId = 312321L;
    byte storagePolicy = 0;
    Map<Integer, String> mapOwnerIdName = new HashMap<>();
    mapOwnerIdName.put(1, "root");
    Map<Integer, String> mapGroupIdName = new HashMap<>();
    mapGroupIdName.put(1, "admin");
    fileInfoDao.updateUsersMap(mapOwnerIdName);
    fileInfoDao.updateGroupsMap(mapGroupIdName);
    FileInfo fileInfo = new FileInfo(path, fileId, length, isDir, blockReplication,
        blockSize, modTime, accessTime, permission, owner, group, storagePolicy);
    fileInfoDao.insert(fileInfo);
    fileInfoDao.update(path, 10);
    FileInfo file = fileInfoDao.getById(fileId);
    fileInfo.setStoragePolicy((byte) 10);
    Assert.assertTrue(file.equals(fileInfo));
  }
}
