package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.ssm.actions.ActionBase;
import org.apache.hadoop.ssm.actions.MoveFile;
import org.apache.hadoop.ssm.actions.MoveToCache;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


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
    init();
    generateTestCase();
    Command cmd = runHelper();
    cmd.runActions();
  }

  private void init() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY,REPLICATION_KEY);
    cluster = new MiniDFSCluster.Builder(conf).
            numDataNodes(3).
            storageTypes(new StorageType[] {StorageType.DISK,StorageType.SSD}).
            build();
    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    client = cluster.getFileSystem().getClient();
  }


  private void generateTestCase() throws Exception {
    final DistributedFileSystem dfs = cluster.getFileSystem();
    Map<String, String> smap1 = new HashMap<String, String>();
    smap1.put("_FILE_PATH_", "/testMoveFile/file1");
    smap1.put("_STORAGE_POLICY_", "ALL_SSD");
    Path dir1 = new Path("/testMoveFile");
    dfs.mkdirs(dir1);
    dfs.setStoragePolicy(dir1, "HOT");
    final FSDataOutputStream out1 = dfs.create(new Path("/testMoveFile/file1"),true,1024);
    out1.writeChars("/testMoveFile/file1");
    out1.close();

    Map<String, String> smap2 = new HashMap<String, String>();
    smap2.put("_FILE_PATH_", "/testMoveFile/file2");
    smap2.put("_STORAGE_POLICY_", "COLD");
    final FSDataOutputStream out2 = dfs.create(new Path("/testMoveFile/file2"),true,1024);
    out2.writeChars("/testMoveFile/file2");
    out2.close();

    Map<String, String> smap3 = new HashMap<String, String>();
    smap3.put("_FILE_PATH_", "/testCacheFile");
    Path dir3 = new Path("/testCacheFile");
    dfs.mkdirs(dir3);
  }

  private Command runHelper() throws Exception {
    ActionBase[] actions = new ActionBase[10];
    String[] args0 = {"/testCacheFile"};
    String[] args1 = {"/testMoveFile/file1"};
    String[] args2 = {"/testMoveFile/file2"};
    // New action


    actions[0] = new MoveToCache(client, conf);
    actions[0].initial(args0);
    actions[1] = new MoveFile(client, conf, "ALL_SSD");
    actions[1].initial(args1);
    actions[2] = new MoveFile(client, conf, "COLD");
    actions[2].initial(args2);

    // New Command
    Command cmd = new Command(actions, null);
    cmd.setId(1);
    cmd.setRuleId(1);
    cmd.setState(CommandState.PENDING);
    // Init action
    return cmd;

  }

}