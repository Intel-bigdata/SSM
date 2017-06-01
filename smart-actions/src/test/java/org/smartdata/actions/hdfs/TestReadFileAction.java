package org.smartdata.actions.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;

public class TestReadFileAction {
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";
  private MiniDFSCluster cluster;
  protected DFSClient client;
  private DistributedFileSystem dfs;
  private Configuration conf = new Configuration();

  @Before
  public void newCluster() throws IOException {
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
    try {
      int bufferSize = 64 * 1024;
      final OutputStream out = client.create(filePath, true);
      // generate random data with given length
      byte[] buffer = new byte[bufferSize];
      new Random().nextBytes(buffer);
      // write to HDFS
      for (int pos = 0; pos < length; pos += bufferSize) {
        int writeLength = pos + bufferSize < length ? bufferSize : length - pos;
        out.write(buffer, 0, writeLength);
      }
      out.close();
    } catch (IOException e) {
      System.err.println("WriteFile Action fails!\n" + e.getMessage());
    }
  }

  @Test
  public void testInit() throws IOException {
    ArrayList<String> args = new ArrayList<>();
    args.add("Test");
    ReadFileAction readFileAction = new ReadFileAction();
    readFileAction.init(args.toArray(new String[args.size()]));
    args.add("1024");
    readFileAction.init(args.toArray(new String[args.size()]));
  }

  @Test
  public void testExecute() throws IOException {
    String filePath = "/testWriteFile/file";
    int size = 66560;
    writeFile(filePath, size);
    String[] args = {filePath};
    ReadFileAction readFileAction = new ReadFileAction();
    readFileAction.setDfsClient(client);
    readFileAction.init(args);
    readFileAction.execute();
  }
}
