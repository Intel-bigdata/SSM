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
import org.junit.Test;
import org.smartdata.actions.hdfs.CacheFileAction;
import org.smartdata.model.ActionInfo;
import org.smartdata.metastore.utils.TestDaoUtil;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestActionDao extends TestDaoUtil {

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
  public void testInsertGetAction() throws Exception {
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, "/test/file");
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213l, true, 123123l,
        100);
    actionDao.insert(new ActionInfo[] {actionInfo});
    actionInfo = actionDao.getById(1l);
    Assert.assertTrue(actionInfo.getCmdletId() == 1);
    // Get wrong id
    try {
      actionInfo = actionDao.getById(100l);
    } catch (EmptyResultDataAccessException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testUpdateAction() throws Exception {
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, "/test/file");
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
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
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, "/test/file");
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213l, true, 123123l,
        100);
    actionDao.insert(actionInfo);
    actionInfo.setActionId(2);
    actionDao.insert(actionInfo);
    List<ActionInfo> actionInfoList = actionDao.getLatestActions(10);
    Assert.assertTrue(actionInfoList.size() == 2);
    actionInfoList = actionDao.getByIds(Arrays.asList(new Long[] {1l, 2l}));
    Assert.assertTrue(actionInfoList.size() == 2);
    actionDao.delete(actionInfo.getActionId());
    actionInfoList = actionDao.getAll();
    Assert.assertTrue(actionInfoList.size() == 1);
  }

  @Test
  public void testMaxId() throws Exception {
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, "/test/file");
    ActionInfo actionInfo = new ActionInfo(1, 1,
        "cache", args, "Test",
        "Test", false, 123213213l, true, 123123l,
        100);
    Assert.assertTrue(actionDao.getMaxId() == 0);
    actionDao.insert(actionInfo);
    Assert.assertTrue(actionDao.getMaxId() == 2);
  }
}
