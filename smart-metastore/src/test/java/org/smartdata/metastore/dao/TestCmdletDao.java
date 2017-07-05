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
import org.smartdata.model.CmdletState;
import org.smartdata.model.CmdletInfo;
import org.smartdata.metastore.utils.TestDaoUtil;
import org.springframework.dao.EmptyResultDataAccessException;

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
        CmdletState.EXECUTING, "test", 123123333l, 232444444l);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333l, 232444994l);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    List<CmdletInfo> cmdlets = cmdletDao.getAll();
    Assert.assertTrue(cmdlets.size() == 2);
  }

  @Test
  public void testUpdateCmdlet() throws Exception {
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333l, 232444444l);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333l, 232444994l);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    cmdlet1.setState(CmdletState.DONE);
    cmdletDao.update(cmdlet1);
    cmdlet1 = cmdletDao.getById(cmdlet1.getCid());
    Assert.assertTrue(cmdlet1.getState() == CmdletState.DONE);
    try {
      cmdletDao.getById(2000l);
    } catch (EmptyResultDataAccessException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testGetByCondition() throws Exception {
    CmdletInfo command1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333l, 232444444l);
    CmdletInfo command2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333l, 232444994l);
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
        CmdletState.EXECUTING, "test", 123123333l, 232444444l);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333l, 232444994l);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    cmdletDao.delete(1);
    List<CmdletInfo> cmdlets = cmdletDao.getAll();
    Assert.assertTrue(cmdlets.size() == 1);
  }

  @Test
  public void testMaxId() throws Exception {
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.EXECUTING, "test", 123123333l, 232444444l);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 78,
        CmdletState.PAUSED, "tt", 123178333l, 232444994l);
    Assert.assertTrue(cmdletDao.getMaxId() == 0);
    cmdletDao.insert(new CmdletInfo[]{cmdlet1, cmdlet2});
    Assert.assertTrue(cmdletDao.getMaxId() == 2);
  }
}
