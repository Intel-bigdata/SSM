/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.smartdata.metastore.utils.TestDaoUtil;
import org.smartdata.model.ClusterConfig;

public class TestClusterConfigDao extends TestDaoUtil {
  private ClusterConfigDao clusterConfigDao;

  @Before
  public void initClusterConfigDao() throws Exception {
    initDao();
    clusterConfigDao = new ClusterConfigDao(druidPool.getDataSource());
  }

  @After
  public void closeClusterConfigDAO() throws Exception {
    closeDao();
    clusterConfigDao = null;
  }

  @Test
  public void testInsertAndGet() {
    ClusterConfig clusterConfig = new ClusterConfig(1, "test", "test");
    clusterConfigDao.insert(clusterConfig);

    Assert.assertTrue(clusterConfigDao.getById(1).equals(clusterConfig));
    Assert.assertTrue(clusterConfigDao.getByName("test").equals(clusterConfig));
  }

  @Test
  public void testUpdate() {
    ClusterConfig clusterConfig = new ClusterConfig(1, "test", "test1");
    clusterConfigDao.insert(clusterConfig);
    clusterConfigDao.updateById(1, "test2");
    clusterConfig.setConfig_path("test2");
    Assert.assertTrue(clusterConfigDao.getById(1).equals(clusterConfig));
  }


}
