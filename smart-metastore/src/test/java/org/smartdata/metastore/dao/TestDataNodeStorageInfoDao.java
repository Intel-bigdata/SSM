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
import org.smartdata.model.DataNodeStorageInfo;

import java.util.List;

public class TestDataNodeStorageInfoDao extends TestDaoUtil {

  private DataNodeStorageInfoDao dataNodeStorageInfoDao;

  @Before
  public void initDataNodeInfoDao() throws Exception {
    initDao();
    dataNodeStorageInfoDao = new DataNodeStorageInfoDao(druidPool.getDataSource());
  }

  @After
  public void closeDataNodeInfoDao() throws Exception {
    closeDao();
    dataNodeStorageInfoDao = null;
  }

  @Test
  public void testInsertGetDataInfo() throws Exception {
    DataNodeStorageInfo insertInfo1 = new DataNodeStorageInfo("uuid", 10, 10,
        "storage_id", 0, 0, 0, 0, 0);
    dataNodeStorageInfoDao.insert(insertInfo1);
    List<DataNodeStorageInfo> getInfo1 = dataNodeStorageInfoDao.getByUuid("uuid");
    Assert.assertTrue(insertInfo1.equals(getInfo1.get(0)));

    DataNodeStorageInfo insertInfo2 = new DataNodeStorageInfo("UUID", 10, 10,
        "STORAGE_ID", 1, 1, 1, 1, 1);
    dataNodeStorageInfoDao.insert(insertInfo2);
    List<DataNodeStorageInfo> getInfo2 = dataNodeStorageInfoDao.getByUuid("UUID");
    Assert.assertTrue(insertInfo2.equals(getInfo2.get(0)));

    List<DataNodeStorageInfo> infos = dataNodeStorageInfoDao.getAll();
    Assert.assertTrue(infos.size() == 2);

    dataNodeStorageInfoDao.delete(insertInfo1.getUuid());
    infos = dataNodeStorageInfoDao.getAll();
    Assert.assertTrue(infos.size() == 1);

    dataNodeStorageInfoDao.deleteAll();
    infos = dataNodeStorageInfoDao.getAll();
    Assert.assertTrue(infos.size() == 0);
  }
}
