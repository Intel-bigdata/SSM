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
package org.smartdata.metastore.dao;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.ActionInfo;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestActionDao extends TestDaoUtil {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private ActionDao actionDao;

  @Before
  public void initActionDao() throws Exception {
    initDao();
    actionDao = new ActionDao(druidPool.getDataSource());
  }

  @After
  public void closeActionDao() throws Exception {
    actionDao = null;
    closeDao();
  }

  @Test
  public void testGetAPageOfAction() {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    ActionInfo actionInfo1 = new ActionInfo(2, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    ActionInfo actionInfo2 = new ActionInfo(3, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    ActionInfo actionInfo3 = new ActionInfo(4, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);

    actionDao.insert(new ActionInfo[]{actionInfo, actionInfo1, actionInfo2, actionInfo3});

    List<String> order = new ArrayList<>();
    order.add("aid");
    List<Boolean> desc = new ArrayList<>();
    desc.add(false);
    Assert.assertTrue(actionDao.getAPageOfAction(2, 1, order, desc).get(0).equals(actionInfo2));
  }

  @Test
  public void testInsertGetAction() throws Exception {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    actionDao.insert(new ActionInfo[]{actionInfo});
    ActionInfo dbActionInfo = actionDao.getById(1L);
    Assert.assertTrue(actionInfo.equals(dbActionInfo));
    // Get wrong id
    expectedException.expect(EmptyResultDataAccessException.class);
    actionDao.getById(100L);
  }

  @Test
  public void testUpdateAction() throws Exception {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    actionDao.insert(actionInfo);
    actionInfo.setSuccessful(true);
    actionDao.update(actionInfo);
    ActionInfo dbActionInfo = actionDao.getById(actionInfo.getActionId());
    Assert.assertTrue(actionInfo.equals(dbActionInfo));
  }

  @Test
  public void testGetNewDeleteAction() throws Exception {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    List<ActionInfo> actionInfoList = actionDao.getLatestActions(0);
    // Get from empty table
    Assert.assertTrue(actionInfoList.size() == 0);
    actionDao.insert(actionInfo);
    actionInfo.setActionId(2);
    actionDao.insert(actionInfo);
    actionInfoList = actionDao.getLatestActions(0);
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getByIds(Arrays.asList(1L, 2L));
    Assert.assertTrue(actionInfoList.size() == 2);
    actionDao.delete(actionInfo.getActionId());
    actionInfoList = actionDao.getAll();
    Assert.assertTrue(actionInfoList.size() == 1);
  }

  @Test
  public void testGetLatestActionListByFinishAndSuccess() {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    List<ActionInfo> actionInfoList =
        actionDao.getLatestActions("cache", 0, false, true);
    //Get from empty table
    Assert.assertTrue(actionInfoList.size() == 0);
    actionDao.insert(actionInfo);
    actionInfo.setActionId(2);
    actionDao.insert(actionInfo);
    actionInfoList = actionDao.getLatestActions("cache", 0, false, true);
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getByIds(Arrays.asList(1L, 2L));
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getLatestActions("cache", 1, false, true);
    Assert.assertTrue(actionInfoList.size() == 1);
  }

  @Test
  public void testGetLatestActionListByFinish() {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    List<ActionInfo> actionInfoList =
        actionDao.getLatestActions("cache", 0);
    //Get from empty table
    Assert.assertTrue(actionInfoList.size() == 0);
    actionDao.insert(actionInfo);
    actionInfo.setActionId(2);
    actionDao.insert(actionInfo);
    actionInfoList = actionDao.getLatestActions("cache", 0, true);
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getByIds(Arrays.asList(1L, 2L));
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getLatestActions("cache", 1, true);
    Assert.assertTrue(actionInfoList.size() == 1);
  }

  @Test
  public void testGetLatestActionListBySuccess() {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    List<ActionInfo> actionInfoList =
        actionDao.getLatestActions("cache", false, 0);
    //Get from empty table
    Assert.assertTrue(actionInfoList.size() == 0);
    actionDao.insert(actionInfo);
    actionInfo.setActionId(2);
    actionDao.insert(actionInfo);
    actionInfoList = actionDao.getLatestActions("cache", false, 0);
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getByIds(Arrays.asList(1L, 2L));
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getLatestActions("cache", false, 1);
    Assert.assertTrue(actionInfoList.size() == 1);
  }


  @Test
  public void testMaxId() throws Exception {
    Map<String, String> args = new HashMap<>();
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213L, true, 123123L,
        100);
    Assert.assertTrue(actionDao.getMaxId() == 0);
    actionDao.insert(actionInfo);
    Assert.assertTrue(actionDao.getMaxId() == 2);
  }
}
