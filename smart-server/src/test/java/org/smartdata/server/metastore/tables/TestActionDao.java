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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.server.metastore.DruidPool;
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.TestDaoUtil;
import org.smartdata.server.metastore.Util;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class TestActionDao extends TestDaoUtil {

  private ActionDao actionDao;

  private void daoInit() {
    actionDao = new ActionDao(druidPool.getDataSource());
  }

  @Test
  public void testInsertGetAction() throws Exception {
    daoInit();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", new String[]{"/test/file"}, "Test",
        "Test", false, 123213213l, true, 123123l,
        100);
    actionDao.insert(actionInfo);
    actionInfo = actionDao.getById(1l);
    Assert.assertTrue(actionInfo.getCommandId() == 1);
  }

  @Test
  public void testUpdateAction() throws Exception {
    daoInit();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", new String[]{"/test/file"}, "Test",
        "Test", false, 123213213l, true, 123123l,
        100);
    actionDao.insert(actionInfo);
    actionInfo.setSuccessful(true);
    actionDao.update(actionInfo);
    actionInfo = actionDao.getById(actionInfo.getActionId());
    Assert.assertTrue(actionInfo.isFinished());
  }

  @Test
  public void testGetNewDeleteAction() throws Exception {
    daoInit();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", new String[]{"/test/file"}, "Test",
        "Test", false, 123213213l, true, 123123l,
        100);
    actionDao.insert(actionInfo);
    actionInfo.setActionId(2);
    actionDao.insert(actionInfo);
    List<ActionInfo> actionInfoList = actionDao.getLatestActions(10);
    Assert.assertTrue(actionInfoList.size() == 2);
    actionDao.delete(actionInfo.getActionId());
    actionInfoList = actionDao.getAll();
    Assert.assertTrue(actionInfoList.size() == 1);
  }

  @Test
  public void testMaxId() throws Exception {
    daoInit();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", new String[]{"/test/file"}, "Test",
        "Test", false, 123213213l, true, 123123l,
        100);
    Assert.assertTrue(actionDao.getMaxId() == 0);
    actionDao.insert(actionInfo);
    Assert.assertTrue(actionDao.getMaxId() == 2);
  }
}
