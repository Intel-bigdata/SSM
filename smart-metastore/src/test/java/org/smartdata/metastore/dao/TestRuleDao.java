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
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;

import java.util.List;

public class TestRuleDao extends TestDaoUtil {

  private RuleDao ruleDao;

  @Before
  public void initRuleDao() throws Exception {
    initDao();
    ruleDao = new RuleDao(druidPool.getDataSource());
  }

  @After
  public void closeRuleDao() throws Exception {
    closeDao();
    ruleDao = null;
  }

  @Test
  public void testInsertGetRule() throws Exception {
    String rule = "file : accessCount(10m) > 20 \n\n"
        + "and length() > 3 | cache";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(0, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    ruleDao.insert(info1);
    RuleInfo info11 = ruleDao.getById(info1.getId());
    Assert.assertTrue(info1.equals(info11));
    RuleInfo info2 = new RuleInfo(1, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    ruleDao.insert(info2);
    RuleInfo info21 = ruleDao.getById(info2.getId());
    Assert.assertFalse(info11.equals(info21));

    List<RuleInfo> infos = ruleDao.getAll();
    Assert.assertTrue(infos.size() == 2);
    ruleDao.delete(info1.getId());
    infos = ruleDao.getAll();
    Assert.assertTrue(infos.size() == 1);
    ruleDao.deleteAll();
    infos = ruleDao.getAll();
    Assert.assertTrue(infos.size() == 0);
  }

  @Test
  public void testUpdateRule() throws Exception {
    String rule = "file : accessCount(10m) > 20 \n\n"
        + "and length() > 3 | cache";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(20L, submitTime,
        rule, RuleState.ACTIVE,
        12, 12, 12);
    ruleDao.insert(info1);
    long rid = ruleDao.update(info1.getId(),
        RuleState.DISABLED.getValue());
    Assert.assertTrue(rid == info1.getId());
    info1 = ruleDao.getById(info1.getId());
    Assert.assertTrue(info1.getNumChecked() == 12L);

    ruleDao.update(rid, System.currentTimeMillis(), 100, 200);
    RuleInfo info2 = ruleDao.getById(rid);
    Assert.assertTrue(info2.getNumChecked() == 100L);
  }
}
