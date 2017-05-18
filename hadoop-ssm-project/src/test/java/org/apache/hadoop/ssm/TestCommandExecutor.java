package org.apache.hadoop.ssm;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.ssm.actions.ActionType;
import org.apache.hadoop.ssm.sql.CommandInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.utils.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CommandExecutor Unit Test
 */
public class TestCommandExecutor extends TestEmptyMiniSSMCluster {

  @Test
  public void testCommandExecutor() throws  Exception {
    generateTestCases();
    testCommandExecutorHelper(ssm.getConf());
  }

  public void generateTestCases() throws Exception {
    try {
      DBAdapter dbAdapter = ssm.getDBAdapter();
      // HDFS related
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
      // DB related
      CommandInfo command1 = new CommandInfo(0, 1, ActionType.MoveFile,
              CommandState.PENDING, JsonUtil.toJsonString(smap1), 123123333l, 232444444l);
      CommandInfo command2 = new CommandInfo(0, 1, ActionType.MoveFile,
              CommandState.PENDING, JsonUtil.toJsonString(smap2), 123178333l, 232444994l);
      CommandInfo command3 = new CommandInfo(0, 1, ActionType.CacheFile,
              CommandState.PENDING, JsonUtil.toJsonString(smap3), 123178333l, 232444994l);
      CommandInfo[] commands = {command1, command2, command3};
      dbAdapter.insertCommandsTable(commands);
      Thread.sleep(10000);
    } finally {
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }


  public void testCommandExecutorHelper(Configuration conf) throws Exception {
    // Check Status
    DBAdapter dbAdapter = ssm.getDBAdapter();
    String cidCondition = ">= 1 ";
    String ridCondition = ">= 1 ";
    List<CommandInfo> com = dbAdapter.getCommandsTableItem(cidCondition, ridCondition, CommandState.DONE);
    System.out.printf("Size = %d\n", com.size());
    Assert.assertTrue(com.size() == 3);
    Assert.assertTrue(com.get(0).getState() == CommandState.DONE);
    // Stop CommandExecutor
    // cmdexe.stop();
  }
}
