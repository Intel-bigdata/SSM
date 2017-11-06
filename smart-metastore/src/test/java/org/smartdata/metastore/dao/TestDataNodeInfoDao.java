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
import org.smartdata.model.DataNodeInfo;

import java.util.List;

public class TestDataNodeInfoDao extends TestDaoUtil {

  private DataNodeInfoDao dataNodeInfoDao;

  @Before
  public void initDataNodeInfoDao() throws Exception {
    initDao();
    dataNodeInfoDao = new DataNodeInfoDao(druidPool.getDataSource());
  }

  @After
  public void closeDataNodeInfoDao() throws Exception {
    closeDao();
    dataNodeInfoDao = null;
  }

  @Test
  public void testInsertGetDataInfo() throws Exception {
    DataNodeInfo insertInfo1 = new DataNodeInfo(
        "UUID1", "hostname", "www.ssm.com",  10000, 50, "lab");
    dataNodeInfoDao.insert(insertInfo1);
    List<DataNodeInfo> getInfo1 = dataNodeInfoDao.getByUuid("UUID1");
    Assert.assertEquals(1, getInfo1.size());
    Assert.assertTrue(insertInfo1.equals(getInfo1.get(0)));

    DataNodeInfo insertInfo2 = new DataNodeInfo(
        "UUID2", "HOSTNAME", "www.ssm.com",  0, 0, null);
    dataNodeInfoDao.insert(insertInfo2);
    List<DataNodeInfo> getInfo2 = dataNodeInfoDao.getByUuid("UUID2");
    Assert.assertEquals(1, getInfo2.size());
    Assert.assertTrue(insertInfo2.equals(getInfo2.get(0)));

    List<DataNodeInfo> infos = dataNodeInfoDao.getAll();
    Assert.assertTrue(infos.size() == 2);

    dataNodeInfoDao.delete(insertInfo1.getUuid());
    infos = dataNodeInfoDao.getAll();
    Assert.assertTrue(infos.size() == 1);

    dataNodeInfoDao.deleteAll();
    infos = dataNodeInfoDao.getAll();
    Assert.assertTrue(infos.size() == 0);
  }

}
