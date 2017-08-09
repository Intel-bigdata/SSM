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
import org.smartdata.model.BackUpInfo;

public class TestBackUpInfoDao extends TestDaoUtil{
  private BackUpInfoDao backUpInfoDao;

  @Before
  public void initBackUpInfoDao() throws Exception {
    initDao();
    backUpInfoDao = new BackUpInfoDao(druidPool.getDataSource());
  }

  @After
  public void closeBackUpInfoDao() throws Exception {
    closeDao();
    backUpInfoDao = null;
  }

  @Test
  public void testInsertAndGetSingleRecord(){
    BackUpInfo backUpInfo = new BackUpInfo();
    backUpInfo.setRid(2);
    backUpInfo.setPeriod(1);
    backUpInfo.setDest("");
    backUpInfo.setSrc("");
    backUpInfoDao.insert(backUpInfo);

    Assert.assertTrue(backUpInfoDao.getById(1).equals(backUpInfo));
  }

  
}
