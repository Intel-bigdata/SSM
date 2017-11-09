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
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.ArrayList;
import java.util.List;

public class TestCmdletDao extends TestDaoUtil {

  private CmdletDao cmdletDao;

  @Before
  public void initCmdletDao() throws Exception {
    initDao();
    cmdletDao = new CmdletDao(druidPool.getDataSource());
  }

  @After
  public void closeCmdletDao() throws Exception {
    closeDao();
    cmdletDao = null;
  }

  @Test
  public void testInsertGetCmdlet() throws Exception {
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    List<CmdletInfo> cmdlets = cmdletDao.getAll();
    Assert.assertTrue(cmdlets.size() == 2);
  }

  @Test
  public void testGetAPageOfAction() {
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});

    List<String> order = new ArrayList<>();
    order.add("cid");
    List<Boolean> desc = new ArrayList<>();
    desc.add(false);

    Assert.assertTrue(cmdletDao.getAPageOfCmdlet(1, 1,
        order, desc).get(0).equals(cmdlet2));
  }

  @Test
  public void testUpdateCmdlet() throws Exception {
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    cmdlet1.setState(CmdletState.DONE);
    cmdletDao.update(cmdlet1);
    CmdletInfo dbcmdlet1 = cmdletDao.getById(cmdlet1.getCid());
    Assert.assertTrue(dbcmdlet1.equals(cmdlet1));
    try {
      cmdletDao.getById(2000L);
    } catch (EmptyResultDataAccessException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testGetByCondition() throws Exception {
    CmdletInfo command1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo command2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    cmdletDao.insert(new CmdletInfo[]{command1, command2});
    List<CmdletInfo> commandInfos = cmdletDao
        .getByCondition(null, null, null);
    Assert.assertTrue(commandInfos.size() == 2);
    commandInfos = cmdletDao
        .getByCondition(null, null, CmdletState.PAUSED);
    Assert.assertTrue(commandInfos.size() == 1);
  }

  @Test
  public void testDeleteACmdlet() throws Exception {
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    cmdletDao.delete(1);
    List<CmdletInfo> cmdlets = cmdletDao.getAll();
    Assert.assertTrue(cmdlets.size() == 1);
  }

  @Test
  public void testMaxId() throws Exception {
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    Assert.assertTrue(cmdletDao.getMaxId() == 0);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    Assert.assertTrue(cmdletDao.getMaxId() == 2);
  }

  @Test
  public void testgetByRid() throws Exception{
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
            CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 1,
            CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    CmdletInfo cmdlet3 = new CmdletInfo(2, 1,
            CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet4 = new CmdletInfo(3, 1,
            CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    CmdletInfo cmdlet5 = new CmdletInfo(4, 1,
            CmdletState.EXECUTING, "test", 123123333L, 232444444L);
    CmdletInfo cmdlet6 = new CmdletInfo(5, 1,
            CmdletState.PAUSED, "tt", 123178333L, 232444994L);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2, cmdlet3, cmdlet4, cmdlet5, cmdlet6});
    List<CmdletInfo> cmdlets = cmdletDao.getByRid(1, 1, 2);
    List<String> order = new ArrayList<>();
    order.add("cid");
    List<Boolean> desc = new ArrayList<>();
    desc.add(false);
    Assert.assertTrue(cmdlets.size() == 2);
    cmdlets = cmdletDao.getByRid(1, 1, 2, order, desc);
    Assert.assertTrue(cmdlets.size() == 2);
    Assert.assertTrue(cmdlets.get(0).equals(cmdlet2));
    Assert.assertTrue(cmdletDao.getNumByRid(1) == 6);
  }

}
