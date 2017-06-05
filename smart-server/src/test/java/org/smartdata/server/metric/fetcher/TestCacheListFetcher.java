package org.smartdata.server.metric.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.junit.Assert;
import org.smartdata.SmartContext;
import org.smartdata.actions.hdfs.CacheFileAction;
import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.Util;

import org.apache.hadoop.hdfs.DFSClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.util.List;


public class TestCacheListFetcher {

  private DBAdapter adapter;
  private String dbFile;
  private Connection conn;

  private CacheListFetcher cacheListFetcher;

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
    cacheListFetcher = new CacheListFetcher(800l, dfsClient, adapter);
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

  @Test
  public void testFetcher() throws Exception {
    String pathPrefix = "/fileTest";
    String[] index = {"1", "2", "3", "4"};
    for (int i = 0; i < index.length; i++) {
      CacheFileAction cacheAction = new CacheFileAction();
      String path = pathPrefix + index[i];
      dfs.mkdirs(new Path(path));
      cacheAction.setContext(smartContext);
      cacheAction.setDfsClient(dfsClient);
      cacheAction.init(new String[] {path});
      cacheAction.run();
    }
    Thread.sleep(1000);
    List<CachedFileStatus> cachedFileStatuses = adapter.getCachedFileStatus();
    Assert.assertTrue(cachedFileStatuses.size() == 4);
    // Uncache files
    // for (int i = 0; i < 2; i++) {
    //   dfs.list
    // }
  }
}
