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
package org.smartdata.hdfs.metric.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.MiniClusterFactory;
import org.smartdata.hdfs.action.CacheFileAction;
import org.smartdata.hdfs.action.UncacheFileAction;
import org.smartdata.model.CachedFileStatus;
import org.smartdata.model.FileInfo;
import org.smartdata.conf.SmartConf;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.TestDaoUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestCachedListFetcher extends TestDaoUtil {

  private MetaStore metaStore;
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
    initDao();
    SmartConf conf = new SmartConf();
    initConf(conf);
    fid = 0l;
    cluster = MiniClusterFactory.get().create(5, conf);
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = dfs.getClient();
    smartContext = new SmartContext(conf);
    metaStore = new MetaStore(druidPool);
    cachedListFetcher = new CachedListFetcher(600l, dfsClient, metaStore);
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
    cachedListFetcher = null;
    closeDao();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private FileInfo createFileStatus(String pathString) {
    long length = 123L;
    boolean isDir = false;
    int blockReplication = 1;
    long blockSize = 128 * 1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    String owner = "root";
    String group = "admin";
    long fileId = fid;
    byte storagePolicy = 0;
    fid++;
    return new FileInfo(pathString, fileId, length,
        isDir, (short)blockReplication, blockSize, modTime, accessTime,
        (short) 1, owner, group, storagePolicy);
  }

  @Test
  public void testFetcher() throws Exception {
    String pathPrefix = "/fileTest/cache/";
    String[] fids = {"5", "7", "9", "10"};
    Path dir = new Path(pathPrefix);
    dfs.mkdirs(dir);
    dfs.setStoragePolicy(dir, "HOT");
    List<FileInfo> fileInfos = new ArrayList<>();
    for (int i = 0; i < fids.length; i++) {
      CacheFileAction cacheAction = new CacheFileAction();
      String path = pathPrefix + fids[i];
      FSDataOutputStream out = dfs.create(new Path(path));
      out.writeChars("testUncache");
      out.close();
      fileInfos.add(createFileStatus(pathPrefix + fids[i]));
      cacheAction.setContext(smartContext);
      cacheAction.setDfsClient(dfsClient);
      Map<String, String> args = new HashMap();
      args.put(CacheFileAction.FILE_PATH, path);
      cacheAction.init(args);
      cacheAction.run();
      // System.out.println(cacheAction.isCached(path));
    }
    metaStore.insertFiles(fileInfos
        .toArray(new FileInfo[fileInfos.size()]));
    List<FileInfo> ret = metaStore.getFile();
    Assert.assertTrue(ret.size() == fids.length);
    cachedListFetcher.start();
    Thread.sleep(1000);
    List<CachedFileStatus> cachedFileStatuses = cachedListFetcher.getCachedList();
    Assert.assertTrue(cachedFileStatuses.size() == fids.length);
    int unCachedSize = 2;
    for (int i = 0; i < unCachedSize; i++) {
      UncacheFileAction uncacheFileAction = new UncacheFileAction();
      String path = pathPrefix + fids[i];
      fileInfos.add(createFileStatus("fileTest/cache/" + fids[i]));
      uncacheFileAction.setContext(smartContext);
      uncacheFileAction.setDfsClient(dfsClient);
      Map<String, String> args = new HashMap();
      args.put(UncacheFileAction.FILE_PATH, path);
      uncacheFileAction.init(args);
      uncacheFileAction.run();
    }
    // System.out.println(uncacheFileAction .isCached(path));
    Thread.sleep(2000);
    cachedFileStatuses = cachedListFetcher.getCachedList();
    Assert.assertTrue(cachedFileStatuses.size() == fids.length - unCachedSize);
  }
}
