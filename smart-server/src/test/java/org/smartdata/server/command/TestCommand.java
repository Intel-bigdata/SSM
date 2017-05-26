package org.smartdata.server.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;

import org.smartdata.common.CommandState;
import org.smartdata.server.actions.Action;
import org.smartdata.server.actions.CacheFile;
import org.smartdata.server.actions.MoveFile;
import org.smartdata.server.actions.mover.MoverPool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Command Unit Test
 */
public class TestCommand {

  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DFSClient client;

  @Test
  public void testRun() throws Exception {
    generateTestCase();
    runHelper().runActions();
    System.out.println("Command UT Finished!!");
  }

  @Before
  public void init() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION_KEY);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(3).
        storagesPerDatanode(4).
        storageTypes(new StorageType[]{StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE, StorageType.RAM_DISK}).
        build();
    cluster.waitActive();
    client = cluster.getFileSystem().getClient();
    MoverPool.getInstance().init(conf);
  }

  @After
  public void shutdown() throws Exception {
    MoverPool.getInstance().shutdown();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public void generateTestCase() throws Exception {
    final DistributedFileSystem dfs = cluster.getFileSystem();
    // New dir
    Path dir = new Path("/testMoveFile");
    dfs.mkdirs(dir);
    // Move to SSD
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out1 = dfs.create(new Path("/testMoveFile/file1"),
        true, 1024);
    out1.writeChars("/testMoveFile/file1");
    out1.close();
    // Move to Archive
    final FSDataOutputStream out2 = dfs.create(new Path("/testMoveFile/file2"),
        true, 1024);
    out2.writeChars("/testMoveFile/file2");
    out2.close();
    // Move to Cache
    Path dir3 = new Path("/testCacheFile");
    dfs.mkdirs(dir3);
  }

  public Command runHelper() throws Exception {
    Action[] actions = new Action[10];
    String[] args1 = {"/testMoveFile/file1", "ALL_SSD"};
    String[] args2 = {"/testMoveFile/file2", "COLD"};
    String[] args3 = {"/testCacheFile"};
    // New action
    actions[0] = new MoveFile(client, conf);
    actions[0].initial(args1);
    actions[1] = new MoveFile(client, conf);
    actions[1].initial(args2);
    actions[2] = new CacheFile(client, conf);
    actions[2].initial(args3);
    // New Command
    Command cmd = new Command(actions, null);
    cmd.setId(1);
    cmd.setRuleId(1);
    cmd.setState(CommandState.PENDING);
    // Init action
    return cmd;
  }
}
