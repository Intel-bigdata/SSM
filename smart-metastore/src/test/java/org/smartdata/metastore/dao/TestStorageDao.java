/**
 * Created by cy on 17-6-19.
 */
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
import org.smartdata.common.models.StorageCapacity;
import org.smartdata.common.models.StoragePolicy;
import org.smartdata.metastore.utils.TestDaoUtil;

import java.util.Map;


public class TestStorageDao extends TestDaoUtil {

  private StorageDao storageDao;

  @Before
  public void initStorageDao() throws Exception {
    initDao();
    storageDao = new StorageDao(druidPool.getDataSource());
  }

  @After
  public void closeStorageDao() throws Exception {
    closeDao();
    storageDao = null;
  }

  @Test
  public void testInsertGetStorageTable() throws Exception {
    StorageCapacity[] storages = new StorageCapacity[2];
    storages[0] = new StorageCapacity("type1", 1l, 1l);
    storages[1] = new StorageCapacity("type2", 2l, 2l);
    storageDao.insertStoragesTable(storages);
    Assert.assertTrue(storageDao.getStorageCapacity("type1").getFree() == 1l);
    Map<String, StorageCapacity> map = storageDao.getStorageTablesItem();
    Assert.assertTrue("type1".equals(map.get("type1").getType()));
  }

  @Test
  public void testInsertGetStorage_policyTable() throws Exception {
    storageDao.insertStoragePolicyTable(new StoragePolicy((byte) 1, "pName"));
    Assert.assertTrue("pName".equals(storageDao.getStoragePolicyName(1)));
    Assert.assertTrue(storageDao.getStoragePolicyID("pName") == 1);
  }

}

