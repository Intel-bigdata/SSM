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
package org.smartdata.server.metastore.tables;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.server.metastore.DruidPool;
import org.smartdata.server.metastore.FileStatusInternal;
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.Util;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class TestFileDao {

  private DruidPool druidPool;
  private FileDao fileDao;

  @Before
  public void init() throws Exception {
    InputStream in = getClass().getClassLoader()
                         .getResourceAsStream("druid-template.xml");
    Properties p = new Properties();
    p.loadFromXML(in);

    String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    String url = Util.SQLITE_URL_PREFIX + dbFile;
    p.setProperty("url", url);

    druidPool = new DruidPool(p);
    fileDao = new FileDao(druidPool.getDataSource());
  }

  @After
  public void shutdown() throws Exception {
    if (druidPool != null) {
      druidPool.close();
    }
  }

  @Test
  public void testInsetGetFiles() throws Exception {
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
    FileStatusInternal fileStatusInternal = new FileStatusInternal(length, isDir, blockReplication,
                                                            blockSize, modTime, accessTime, perms, owner, group, symlink,
                                                            path, "/tmp", fileId, numChildren, null, storagePolicy);
    fileDao.insert(fileStatusInternal);
    HdfsFileStatus hdfsFileStatus = fileDao.getByPath("/tmp/testFile");
    Assert.assertTrue(hdfsFileStatus.getBlockSize() == fileStatusInternal.getBlockSize());
    hdfsFileStatus = fileDao.getById(312321L);
    Assert.assertTrue(hdfsFileStatus.getModificationTime() == fileStatusInternal.getModificationTime());
    fileStatusInternal = new FileStatusInternal(length, isDir, blockReplication,
                                                   blockSize, modTime, accessTime, perms, owner, group, symlink,
                                                   path, "/tmp2", fileId + 1, numChildren, null, storagePolicy);
    fileDao.insert(new FileStatusInternal[] {fileStatusInternal});
  }

  @Test
  public void testInseDeleteFiles() throws Exception {
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
    FileStatusInternal fileStatusInternal1 = new FileStatusInternal(length, isDir, blockReplication,
                                                                      blockSize, modTime, accessTime, perms, owner, group, symlink,
                                                                      path, "/tmp", fileId, numChildren, null, storagePolicy);
    FileStatusInternal fileStatusInternal2 = new FileStatusInternal(length, isDir, blockReplication,
                                                   blockSize, modTime, accessTime, perms, owner, group, symlink,
                                                   path, "/tmp2", fileId + 1, numChildren, null, storagePolicy);
    fileDao.insert(new FileStatusInternal[] {fileStatusInternal1, fileStatusInternal2});
    List<FileStatusInternal> files = fileDao.getAll();
    Assert.assertTrue(files.size() == 2);
    fileDao.deleteById(fileStatusInternal1.getFileId());
    files = fileDao.getAll();
    Assert.assertTrue(files.size() == 1);
    fileDao.deleteAll();
    files = fileDao.getAll();
    Assert.assertTrue(files.size() == 0);
  }

  @Test
  public void testInseUpdateFiles() throws Exception {
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
    FileStatusInternal fileStatusInternal = new FileStatusInternal(length, isDir, blockReplication,
                                                                       blockSize, modTime, accessTime, perms, owner, group, symlink,
                                                                       path, "/tmp", fileId, numChildren, null, storagePolicy);
    fileDao.insert(fileStatusInternal);
    fileDao.update("/tmp/testFile", 10);
    fileStatusInternal = fileDao.getById(312321L);
    Assert.assertTrue(fileStatusInternal.getStoragePolicy() == 10);
  }

}
