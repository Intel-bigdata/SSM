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
import org.smartdata.metastore.TestDaoUtil;

import java.sql.SQLException;
import java.util.List;

public class TestOtherDao extends TestDaoUtil {
  private GroupsDao groupsDao;
  private UserDao userDao;
  private XattrDao xattrDao;

  @Before
  public void initOtherDao() throws Exception {
    initDao();
    groupsDao = new GroupsDao(druidPool.getDataSource());
    userDao = new UserDao(druidPool.getDataSource());
    xattrDao = new XattrDao(druidPool.getDataSource());
  }

  @After
  public void closeOtherDao() throws Exception {
    closeDao();
    groupsDao = null;
    userDao = null;
    xattrDao = null;
  }

  @Test
  public void testGroup() throws SQLException {
    int i = groupsDao.getCountGroups();
    groupsDao.addGroup("groupname111");
    int i1 = groupsDao.getCountGroups();
    groupsDao.deleteGroup("groupname111");
    int i2 = groupsDao.getCountGroups();
    groupsDao.getGroupsMap();
    List<String> list = groupsDao.listGroup();
    Assert.assertTrue(i == i2);
    Assert.assertTrue(i1 == i + 1);
  }

  @Test
  public void testUser() throws SQLException {
    int i = userDao.getCountUsers();
    userDao.addUser("username");
    int i1 = userDao.getCountUsers();
    userDao.deleteUser("username");
    int i2 = userDao.getCountUsers();
    userDao.getUsersMap();
    List<String> list = userDao.listUser();
    Assert.assertTrue(i == i2);
    Assert.assertTrue(i1 == i + 1);
  }
}
