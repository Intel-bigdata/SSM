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
package org.smartdata.server.metric.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.junit.Assert;
import org.smartdata.SmartContext;
import org.smartdata.actions.hdfs.CacheFileAction;
import org.smartdata.actions.hdfs.UncacheFileAction;
import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.metastore.FileStatusInternal;
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.Util;

import org.apache.hadoop.hdfs.DFSClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;


public class TestCachedListFetcher {

  private DBAdapter adapter;
  private String dbFile;
  private Connection conn;
  private long fid;

  private CachedListFetcher cachedListFetcher;

  private static final int DEFAULT_BLOCK_SIZE = 50;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem dfs;
  protected DFSClient dfsClient;
  protected SmartContext smartContext;

  static {
    TestBalancer.initTestSetup();
  }

  @Before
  public void init() throws Exception {
    SmartConf conf = new SmartConf();
    initConf(conf);
    fid = 0l;
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(5)
        .storagesPerDatanode(3)
        .storageTypes(new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
            StorageType.SSD})
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = dfs.getClient();
    smartContext = new SmartContext(conf);
    dbFile = TestDBUtil.getUniqueDBFilePath();
    conn = TestDBUtil.getTestDBInstance();
    Util.initializeDataBase(conn);
    adapter = new DBAdapter(conn);
    cachedListFetcher = new CachedListFetcher(600l, dfsClient, adapter);
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  @After
  public void shutdown() throws Exception {
    cachedListFetcher.stop();
    if (cluster != null) {
      cluster.shutdown();
    }
    if (conn != null) {
      conn.close();
    }
    if (dbFile != null) {
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }

  private FileStatusInternal createFileStatus(String pathSting) {
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
    byte[] path = DFSUtil.string2Bytes(pathSting);
    long fileId = fid;
    fid++;
    int numChildren = 0;
    byte storagePolicy = 0;
    return new FileStatusInternal(length, isDir, blockReplication,
        blockSize, modTime, accessTime, perms, owner, group, symlink,
        path, "", fileId, numChildren, null, storagePolicy);
  }

  @Test
  public void testFetcher() throws Exception {
    String pathPrefix = "/fileTest/cache/";
    String[] fids = {"5", "7", "9", "10"};
    Path dir = new Path(pathPrefix);
    dfs.mkdirs(dir);
    dfs.setStoragePolicy(dir, "HOT");
    List<FileStatusInternal> fileStatusInternals = new ArrayList<>();
    for (int i = 0; i < fids.length; i++) {
      CacheFileAction cacheAction = new CacheFileAction();
      String path = pathPrefix + fids[i];
      FSDataOutputStream out = dfs.create(new Path(path));
      out.writeChars("testUncache");
      out.close();
      fileStatusInternals.add(createFileStatus("fileTest/cache/" + fids[i]));
      cacheAction.setContext(smartContext);
      cacheAction.setDfsClient(dfsClient);
      cacheAction.init(new String[]{path});
      cacheAction.run();
      // System.out.println(cacheAction.isCached(path));
    }
    adapter.insertFiles(fileStatusInternals
        .toArray(new FileStatusInternal[fileStatusInternals.size()]));
    List<HdfsFileStatus> ret = adapter.getFile();
    Assert.assertTrue(ret.size() == fids.length);
    cachedListFetcher.start();
    Thread.sleep(1000);
    List<CachedFileStatus> cachedFileStatuses = cachedListFetcher.getCachedList();
    Assert.assertTrue(cachedFileStatuses.size() == fids.length);
    int unCachedSize = 2;
    for (int i = 0; i < unCachedSize; i++) {
      UncacheFileAction uncacheFileAction = new UncacheFileAction();
      String path = pathPrefix + fids[i];
      fileStatusInternals.add(createFileStatus("fileTest/cache/" + fids[i]));
      uncacheFileAction.setContext(smartContext);
      uncacheFileAction.setDfsClient(dfsClient);
      uncacheFileAction.init(new String[]{path});
      uncacheFileAction.run();
    }
    // System.out.println(uncacheFileAction .isCached(path));
    Thread.sleep(2000);
    cachedFileStatuses = cachedListFetcher.getCachedList();
    Assert.assertTrue(cachedFileStatuses.size() == fids.length - unCachedSize);
  }
}
