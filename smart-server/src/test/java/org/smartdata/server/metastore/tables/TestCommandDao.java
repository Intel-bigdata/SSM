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
package org.smartdata.server.metastore.tables;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.common.CommandState;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.server.metastore.TestDaoUtil;

import java.util.List;

public class TestCommandDao extends TestDaoUtil {

  private CommandDao commandDao;

  private void daoInit() {
    commandDao = new CommandDao(druidPool.getDataSource());
  }

  @Test
  public void testInsertGetCommand() throws Exception {
    daoInit();
    CommandInfo command1 = new CommandInfo(0, 1,
        CommandState.EXECUTING, "test", 123123333l, 232444444l);
    CommandInfo command2 = new CommandInfo(1, 78,
        CommandState.PAUSED, "tt", 123178333l, 232444994l);
    commandDao.insert(new CommandInfo[]{command1, command2});
    List<CommandInfo> commands = commandDao.getAll();
    Assert.assertTrue(commands.size() == 2);
  }

  @Test
  public void testUpdateCommand() throws Exception {
    daoInit();
    CommandInfo command1 = new CommandInfo(0, 1,
        CommandState.EXECUTING, "test", 123123333l, 232444444l);
    CommandInfo command2 = new CommandInfo(1, 78,
        CommandState.PAUSED, "tt", 123178333l, 232444994l);
    commandDao.insert(new CommandInfo[]{command1, command2});
    command1.setState(CommandState.DONE);
    commandDao.update(command1);
    command1 = commandDao.getById(command1.getCid());
    Assert.assertTrue(command1.getState() == CommandState.DONE);
  }

  @Test
  public void testDeleteACommand() throws Exception {
    daoInit();
    CommandInfo command1 = new CommandInfo(0, 1,
        CommandState.EXECUTING, "test", 123123333l, 232444444l);
    CommandInfo command2 = new CommandInfo(1, 78,
        CommandState.PAUSED, "tt", 123178333l, 232444994l);
    commandDao.insert(new CommandInfo[]{command1, command2});
    commandDao.delete(1);
    List<CommandInfo> commands = commandDao.getAll();
    Assert.assertTrue(commands.size() == 1);
  }

  @Test
  public void testMaxId() throws Exception {
    daoInit();
    CommandInfo command1 = new CommandInfo(0, 1,
        CommandState.EXECUTING, "test", 123123333l, 232444444l);
    CommandInfo command2 = new CommandInfo(1, 78,
        CommandState.PAUSED, "tt", 123178333l, 232444994l);
    Assert.assertTrue(commandDao.getMaxId() == 0);
    commandDao.insert(new CommandInfo[]{command1, command2});
    Assert.assertTrue(commandDao.getMaxId() == 2);
  }
}
