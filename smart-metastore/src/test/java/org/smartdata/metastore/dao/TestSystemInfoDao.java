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
import org.smartdata.model.SystemInfo;

import java.util.List;

public class TestSystemInfoDao extends TestDaoUtil {
  private SystemInfoDao systemInfoDao;

  @Before
  public void initSystemInfoDao() throws Exception {
    initDao();
    systemInfoDao = new SystemInfoDao(druidPool.getDataSource());
  }

  @After
  public void closeSystemInfoDao() throws Exception {
    closeDao();
    systemInfoDao = null;
  }

  @Test
  public void testInsertAndGet() {
    SystemInfo systemInfo = new SystemInfo("test", "test");

    systemInfoDao.insert(systemInfo);
    Assert.assertTrue(systemInfoDao.getByProperty("test").equals(systemInfo));
  }

  @Test
  public void testBatchInsertAndQuery() {
    SystemInfo[] systemInfos = new SystemInfo[2];
    systemInfos[0] = new SystemInfo("test", "test");
    systemInfos[1] = new SystemInfo("test1", "test1");

    systemInfoDao.insert(systemInfos);

    List<SystemInfo> systemInfoList = systemInfoDao.getAll();
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(systemInfoList.get(i).equals(systemInfos[i]));
    }
  }

  @Test
  public void testUpdate() {
    SystemInfo systemInfo = new SystemInfo("test", "test");
    systemInfoDao.insert(systemInfo);

    SystemInfo newSystemInfo = new SystemInfo("test", "test1");
    systemInfoDao.update(newSystemInfo);
    Assert.assertTrue(systemInfoDao.getByProperty("test").equals(newSystemInfo));
  }
}
