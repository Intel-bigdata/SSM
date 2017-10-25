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
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.StorageCapacity;
import org.smartdata.model.StoragePolicy;

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
    StorageCapacity[] storageCapacities = new StorageCapacity[2];
    storageCapacities[0] = new StorageCapacity("type1", 1L, 1L);
    storageCapacities[1] = new StorageCapacity("type2", 2L, 2L);
    storageDao.insertUpdateStoragesTable(storageCapacities);
    Assert.assertTrue(storageDao.getStorageCapacity("type1").equals(storageCapacities[0]));
    Map<String, StorageCapacity> map = storageDao.getStorageTablesItem();
    Assert.assertTrue(map.get("type2").equals(storageCapacities[1]));
  }

  @Test
  public void testInsertGetStorage_policyTable() throws Exception {
    StoragePolicy storagePolicy = new StoragePolicy((byte) 1, "pName");
    storageDao.insertStoragePolicyTable(storagePolicy);
    Assert.assertTrue(storageDao.getStoragePolicyName(1).equals("pName"));
    Assert.assertTrue(storageDao.getStoragePolicyID("pName") == 1);
  }

}

