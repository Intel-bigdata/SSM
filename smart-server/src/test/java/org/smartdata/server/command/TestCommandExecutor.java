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
package org.smartdata.server.command;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.SmartAction;
import org.smartdata.common.CommandState;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.common.actions.ActionType;
import org.smartdata.server.TestEmptyMiniSmartCluster;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.utils.JsonUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CommandExecutor Unit Test
 */
public class TestCommandExecutor extends TestEmptyMiniSmartCluster {

  @Test
  public void testCreateFromDescriptor() throws Exception {
    waitTillSSMExitSafeMode();
    generateTestCases();
    CommandDescriptor commandDescriptor = generateCommandDescriptor();
    SmartAction[] actions = ssm.getCommandExecutor().createActionsFromParameters(commandDescriptor);
    Assert.assertTrue(commandDescriptor.size() == actions.length);
  }


  /*@Test
  public void testCommandExecutor() throws Exception {
    waitTillSSMExitSafeMode();
    generateTestFiles();
    generateTestCases();
    testCommandExecutorHelper();
  }

  /*@Test
  public void testGetListDeleteCommand() throws Exception {
    waitTillSSMExitSafeMode();
    generateTestCases();
    Assert.assertTrue(ssm
        .getCommandExecutor()
        .listCommandsInfo(1, null).size() == 1);
    Assert.assertTrue(ssm
        .getCommandExecutor().getCommandInfo(1) != null);
    ssm.getCommandExecutor().deleteCommand(1);
    Assert.assertTrue(ssm
        .getCommandExecutor()
        .listCommandsInfo(1, null).size() == 1);
  }

  @Test
  public void testActivateDisableCommand() throws Exception {
    waitTillSSMExitSafeMode();
    generateTestFiles();
    generateTestCases();
    // Activate 1
    ssm.getCommandExecutor().activateCommand(1);
    Assert.assertTrue(ssm.getCommandExecutor().inCache(1));
    // Disable 1
    CommandInfo cmdinfo = ssm.getCommandExecutor().getCommandInfo(1);
    if (cmdinfo.getState() != CommandState.DONE) {
      ssm.getCommandExecutor().disableCommand(1);
      Assert.assertTrue(cmdinfo.getState() == CommandState.DISABLED);
    }
  }*/

  private void generateTestFiles() throws IOException {
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

  private CommandDescriptor generateCommandDescriptor() {
    CommandDescriptor commandDescriptor = new CommandDescriptor();
    commandDescriptor.setRuleId(1);
    commandDescriptor.addAction("allssd", new String[]{"/testMoveFile/file1"});
    commandDescriptor.addAction("cache", new String[]{"/testCacheFile"});
    return commandDescriptor;
  }

  private void generateTestCases() throws Exception {
    DBAdapter dbAdapter = ssm.getDBAdapter();
    CommandDescriptor commandDescriptor = generateCommandDescriptor();
    CommandInfo commandInfo = new CommandInfo(0, 1, ActionType.CacheFile,
        CommandState.PENDING, commandDescriptor.toString(),
        123178333l, 232444994l);
    CommandInfo[] commands = {commandInfo};
    dbAdapter.insertCommandsTable(commands);
  }

  private void testCommandExecutorHelper() throws Exception {
    DBAdapter dbAdapter = ssm.getDBAdapter();
    String cidCondition = ">= 1 ";
    String ridCondition = ">= 1 ";
    while(true) {
      Thread.sleep(2000);
      int current = ssm.getCommandExecutor().cacheSize();
      System.out.printf("Command Cache size = %d\n ", current);
      if (current == 0) {
        break;
      }
    }
    List<CommandInfo> com = dbAdapter.getCommandsTableItem(cidCondition,
        ridCondition, CommandState.DONE);
    System.out.printf("Size = %d\n", com.size());
    // Check Status
    Assert.assertTrue(com.size() == 1);
    Assert.assertTrue(com.get(0).getState() == CommandState.DONE);
  }
}
