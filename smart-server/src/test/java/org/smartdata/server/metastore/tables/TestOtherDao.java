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
import org.smartdata.server.metastore.DruidPool;
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.Util;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class TestOtherDao {
  private DruidPool druidPool;
  private GroupsDao groupsDao;
  private UserDao userDao;
  private XattrDao xattrDao;

  @Before
  public void init() throws Exception {
    InputStream in = getClass().getClassLoader()
        .getResourceAsStream("druid-template.xml");
    Properties p = new Properties();
    p.loadFromXML(in);

    String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    String url = Util.SQLITE_URL_PREFIX + dbFile;
    p.setProperty("url", url);

    druidPool = new DruidPool(p);
    groupsDao = new GroupsDao(druidPool.getDataSource());
    userDao = new UserDao(druidPool.getDataSource());
    xattrDao = new XattrDao(druidPool.getDataSource());
  }

  @Test
  public void testGroup() throws SQLException {
    groupsDao.addGroup("groupname111");
    groupsDao.addGroup("groupname112");
    groupsDao.updateGroupsMap();
  }

  @Test
  public void testUser() throws SQLException {
    userDao.addUser("username1");
    userDao.addUser("username2");
    userDao.updateUsersMap();
  }

  @Test
  public void testXattr() throws SQLException {
    long fid = 567l;
    Map<String, byte[]> xAttrMap = new HashMap<>();
    String name1 = "user.a1";
    String name2 = "raw.you";
    Random random = new Random();
    byte[] value1 = new byte[1024];
    byte[] value2 = new byte[1024];
    random.nextBytes(value1);
    random.nextBytes(value2);
    xAttrMap.put(name1, value1);
    xAttrMap.put(name2, value2);
    Assert.assertTrue(xattrDao.insertXattrTable(fid, xAttrMap));
    Map<String, byte[]> map = xattrDao.getXattrTable(fid);
    Assert.assertTrue(map.size() == xAttrMap.size());
    for (String m : map.keySet()) {
      Assert.assertArrayEquals(map.get(m), xAttrMap.get(m));
    }
  }

  @Test
  public void testStorage(){
  }

  @After
  public void shutdown() throws Exception {
    if (druidPool != null) {
      druidPool.close();
    }
  }
}
