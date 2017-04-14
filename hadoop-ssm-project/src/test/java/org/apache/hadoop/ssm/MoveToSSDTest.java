package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.ssm.actions.MoveToSSD;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by cc on 17-1-12.
 */
public class MoveToSSDTest {
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";

  private void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY,REPLICATION_KEY);
  }
  @Test
  public void MoveToSSD() throws Exception{
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    testMoveToSSD(conf);
  }

  private void testMoveToSSD(Configuration conf)throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).storageTypes(new StorageType[] {StorageType.DISK,StorageType.SSD}).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveToSSD/file";
      Path dir = new Path("/testMoveToSSD");
      final DFSClient client = cluster.getFileSystem().getClient();
      dfs.mkdirs(dir);

      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out = dfs.create(new Path(file),true,1024);
      out.writeChars(file);
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      String[] str = {file};
      MoveToSSD.getInstance(client, conf).initial(str);
      MoveToSSD.getInstance(client, conf).execute();
      // verify after movement
      LocatedBlock lb1 = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes1 = lb1.getStorageTypes();
      for (StorageType storageType : storageTypes1) {
        Assert.assertTrue(StorageType.SSD == storageType);
      }
    } finally {
      cluster.shutdown();
    }
  }
}