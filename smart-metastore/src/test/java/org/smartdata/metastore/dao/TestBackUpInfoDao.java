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
import org.smartdata.model.BackUpInfo;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.List;

public class TestBackUpInfoDao extends TestDaoUtil {
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
  public void testInsertAndGetSingleRecord() {
    BackUpInfo backUpInfo = new BackUpInfo();
    backUpInfo.setRid(1);
    backUpInfo.setPeriod(1);
    backUpInfo.setDest("");
    backUpInfo.setSrc("");
    backUpInfoDao.insert(backUpInfo);

    Assert.assertTrue(backUpInfoDao.getByRid(1).equals(backUpInfo));
  }

  @Test
  public void testDelete() {
    backUpInfoDao.delete(1L);
    BackUpInfo[] backUpInfos = new BackUpInfo[2];
    backUpInfos[0] = new BackUpInfo(1, "test", "test", 1);
    backUpInfos[1] = new BackUpInfo(2, "test", "test", 1);

    backUpInfoDao.insert(backUpInfos);
    backUpInfoDao.delete(1L);
    Assert.assertTrue(backUpInfoDao.getByRid(2).equals(backUpInfos[1]));
    try {
      backUpInfoDao.getByRid(1);
    } catch (EmptyResultDataAccessException e) {
    }
  }

  @Test
  public void testBatchInsert() {
    BackUpInfo[] backUpInfos = new BackUpInfo[2];
    backUpInfos[0] = new BackUpInfo(1, "test", "test", 1);
    backUpInfos[1] = new BackUpInfo(2, "test", "test", 1);

    backUpInfoDao.insert(backUpInfos);


    Assert.assertTrue(backUpInfoDao.getByRid(1).equals(backUpInfos[0]));
    Assert.assertTrue(backUpInfoDao.getByRid(2).equals(backUpInfos[1]));
  }

  @Test
  public void testUpdate() {
    BackUpInfo backUpInfo = new BackUpInfo();
    backUpInfo.setRid(1);
    backUpInfo.setSrc("test");
    backUpInfo.setDest("test");
    backUpInfo.setPeriod(1);

    backUpInfoDao.insert(backUpInfo);
    backUpInfoDao.update(1, 2);
    backUpInfo.setPeriod(2);
    Assert.assertTrue(backUpInfoDao.getByRid(1).equals(backUpInfo));
  }

  @Test
  public void testgetBySrc() {
    Assert.assertTrue(backUpInfoDao.getByDest("1").size() == 0);
    BackUpInfo[] backUpInfos = new BackUpInfo[2];
    backUpInfos[0] = new BackUpInfo(1, "test", "test", 1);
    backUpInfos[1] = new BackUpInfo(2, "test", "test", 1);

    backUpInfoDao.insert(backUpInfos);
    List<BackUpInfo> list = backUpInfoDao.getBySrc("test");
    Assert.assertTrue(list.size() == 2);
    Assert.assertTrue(list.get(0).equals(backUpInfos[0]));
    Assert.assertTrue(list.get(1).equals(backUpInfos[1]));
    Assert.assertTrue(backUpInfoDao.getCountByRid(1) == 0);
  }
}
