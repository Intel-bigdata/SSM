package org.apache.hadoop.ssm;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ssm.actions.ActionType;
import org.apache.hadoop.ssm.sql.CommandInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.TestDBUtil;
import org.apache.hadoop.ssm.sql.Util;
import org.apache.hadoop.ssm.utils.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CommandExecutor Unit Test
 */
public class CommandExecutorTest {
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";

  private void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY,REPLICATION_KEY);
  }

  @Test
  public void testCommandExecutor() throws  Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    testCommandExecutorHelper(conf);
  }

  public void testCommandExecutorHelper(Configuration conf) throws Exception {
    SSMServer ssm = SSMServer.createSSM(null, conf);
    CommandExecutor cmdexe = new CommandExecutor(ssm, conf);
    // Init Database
    String dbFile = TestDBUtil.getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = Util.createSqliteConnection(dbFile);
      Util.initializeDataBase(conn);
      DBAdapter dbAdapter = new DBAdapter(conn);
      cmdexe.init(dbAdapter);
      Map<String, String> smap = new HashMap<String, String>();
      smap.put("_FILE_PATH_", "");
      smap.put("_STORAGE_POLICY_", "ALL_SSD");
      CommandInfo command1 = new CommandInfo(0, 1, ActionType.None,
              CommandState.PENDING, JsonUtil.toJsonString(smap), 123123333l, 232444444l);
      CommandInfo command2 = new CommandInfo(0, 78, ActionType.ConvertToEC,
              CommandState.PENDING, JsonUtil.toJsonString(smap), 123178333l, 232444994l);
      CommandInfo[] commands = {command1, command2};
      dbAdapter.insertCommandsTable(commands);
      // start CommandExecutor
      cmdexe.start();
      // Stop CommandExecutor
      Thread.sleep(4000);
      cmdexe.stop();
      // Check Status
      String cidCondition = ">= 1 ";
      String ridCondition = "= 78 ";
      List<CommandInfo> com = dbAdapter.getCommandsTableItem(cidCondition, ridCondition, CommandState.PENDING);
      Assert.assertTrue(com.size() == 1);
      Assert.assertTrue(com.get(0).getState() == CommandState.DONE);

    } finally {
      if (conn != null) {
        conn.close();
      }
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }
}
