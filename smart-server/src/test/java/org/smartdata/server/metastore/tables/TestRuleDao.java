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
package org.smartdata.server.metastore.tables;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.server.metastore.TestDaoUtil;

import java.util.List;

public class TestRuleDao extends TestDaoUtil {

  private RuleDao ruleDao;

  private void daoInit() {
    ruleDao = new RuleDao(druidPool.getDataSource());
  }

  @Test
  public void testInsertGetRule() throws Exception {
    daoInit();
    String rule = "file : accessCount(10m) > 20 \n\n"
        + "and length() > 3 | cache";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(0, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    ruleDao.insert(info1);
    RuleInfo info1_1 = ruleDao.getById(info1.getId());
    Assert.assertTrue(info1.equals(info1_1));
    RuleInfo info2 = new RuleInfo(1, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    ruleDao.insert(info2);
    RuleInfo info2_1 = ruleDao.getById(info2.getId());
    Assert.assertFalse(info1_1.equals(info2_1));

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
    daoInit();
    String rule = "file : accessCount(10m) > 20 \n\n"
        + "and length() > 3 | cache";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(0, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    ruleDao.insert(info1);
    ruleDao.update(0, RuleState.DISABLED, 12l, 12l, 12);
    info1 = ruleDao.getById(0);
    Assert.assertTrue(info1.getLastCheckTime() == 12l);
  }
}
