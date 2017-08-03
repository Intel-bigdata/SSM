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
import org.smartdata.metastore.utils.TestDaoUtil;
import org.smartdata.model.GlobalConfig;

public class TestGlobalConfigDao extends TestDaoUtil{
  private GlobalConfigDao globalConfigDao;

  @Before
  public void initGlobalConfigDao() throws Exception {
    initDao();
    globalConfigDao = new GlobalConfigDao(druidPool.getDataSource());
  }

  @After
  public void closeGlobalConfigDao() throws Exception {
    closeDao();
    globalConfigDao = null;
  }

  @Test
  public void testInsertAndGetSingleRecord(){
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setCid(1);
    globalConfig.setProperty_name("test");
    globalConfig.setProperty_value("test1");
    globalConfigDao.insert(globalConfig);

    Assert.assertTrue(globalConfigDao.getById(1).equals(globalConfig));
  }

  @Test
  public void testBatchInsert() {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setCid(1);
    globalConfig.setProperty_name("test");
    globalConfig.setProperty_value("test1");


    GlobalConfig[] globalConfigs = new GlobalConfig[1];
    globalConfigs[0] = globalConfig;

    globalConfigDao.insert(globalConfigs);

    Assert.assertTrue(globalConfigDao.getById(1).equals(globalConfig));
  }

  @Test
  public void testUpdate() {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setCid(1);
    globalConfig.setProperty_name("test");
    globalConfig.setProperty_value("test1");
    globalConfigDao.insert(globalConfig);

    globalConfigDao.update("test", "test2");
    globalConfig.setProperty_value("test2");

    Assert.assertTrue(globalConfigDao.getById(1).equals(globalConfig));
  }
}
