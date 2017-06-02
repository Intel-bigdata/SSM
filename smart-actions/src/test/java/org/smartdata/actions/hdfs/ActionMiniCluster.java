package org.smartdata.actions.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.junit.After;
import org.junit.Before;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;

import java.io.IOException;

/**
 * A MiniCluster for action test.
 */
public abstract class ActionMiniCluster {
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
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
