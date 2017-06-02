package org.smartdata.actions.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;


public class TestWriteFileAction {
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";
  private MiniDFSCluster cluster;
  protected DFSClient client;
  private DistributedFileSystem dfs;
  private Configuration conf = new Configuration();

  @Before
  public void createCluster() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION_KEY);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(3).
        storageTypes(new StorageType[]
            {StorageType.DISK, StorageType.ARCHIVE}).
        build();
    client = cluster.getFileSystem().getClient();
    dfs = cluster.getFileSystem();
    cluster.waitActive();
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  protected void writeFile(String filePath, int length) throws IOException {
    String[] args = {filePath, String.valueOf(length)};
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.setDfsClient(client);
    writeFileAction.init(args);
    writeFileAction.execute();
  }

  @Test
  public void testInit() throws IOException {
    ArrayList<String> args = new ArrayList<>();
    args.add("Test");
    args.add("10");
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.init(args.toArray(new String[args.size()]));
    args.add("1024");
    writeFileAction.init(args.toArray(new String[args.size()]));
  }

  @Test
  public void testExecute() throws Exception {
    String filePath = "/testWriteFile/file";
    int size = 66560;
    writeFile(filePath, size);
    HdfsFileStatus fileStatus = dfs.getClient().getFileInfo(filePath);
    Assert.assertTrue(fileStatus.getLen() == size);
  }
}
