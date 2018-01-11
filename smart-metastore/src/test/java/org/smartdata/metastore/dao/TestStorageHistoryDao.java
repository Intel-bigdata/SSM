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

import java.util.List;


public class TestStorageHistoryDao extends TestDaoUtil {

  private StorageHistoryDao storageHistDao;

  @Before
  public void initStorageDao() throws Exception {
    initDao();
    storageHistDao = new StorageHistoryDao(druidPool.getDataSource());
  }

  @After
  public void closeStorageDao() throws Exception {
    closeDao();
    storageHistDao = null;
  }

  @Test
  public void testInsertGetStorageTable() throws Exception {
    StorageCapacity[] storageCapacities = new StorageCapacity[3];
    storageCapacities[0] = new StorageCapacity("type1", 1000L, 10L, 1L);
    storageCapacities[1] = new StorageCapacity("type1", 2000L, 10L, 2L);
    storageCapacities[2] = new StorageCapacity("type1", 3000L, 10L, 3L);
    storageHistDao.insertStorageHistTable(storageCapacities, 1000);
    List<StorageCapacity> capacities = storageHistDao.getStorageHistoryData(
        "type1", 1000, 1000, 3000);
    Assert.assertTrue(capacities.size() == storageCapacities.length);
    Assert.assertEquals(storageCapacities.length,
        storageHistDao.getNumberOfStorageHistoryData("type1", 1000));
    storageHistDao.deleteOldRecords("type1", 1000, 1000L);
    Assert.assertEquals(storageCapacities.length - 1,
        storageHistDao.getNumberOfStorageHistoryData("type1", 1000));
  }
}

