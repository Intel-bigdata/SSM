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
package org.apache.hadoop.ssm.rule;

import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.TestDBUtil;
import org.apache.hadoop.ssm.sql.Util;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;

/**
 * Testing RuleManager service.
 */
public class TestRuleManager {
  private RuleManager ruleManager;
  private DBAdapter dbAdapter;

  @Before
  public void init() throws Exception {
    String dbFile = TestDBUtil.getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = Util.createSqliteConnection(dbFile);
      Util.initializeDataBase(conn);
      dbAdapter = new DBAdapter(conn);
      // TODO: to be fixed
      ruleManager = new RuleManager(null, null, dbAdapter);
    } finally {
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }

  @Test
  public void testSubmitNewActiveRule() throws Exception {
    String rule = "file: every 1s \n | length > 300 | cachefile";
    long id = ruleManager.submitRule(rule, RuleState.ACTIVE);
    RuleInfo ruleInfo = ruleManager.getRuleInfo(id);
    Assert.assertTrue(ruleInfo.getRuleText().equals(rule));
    RuleInfo info = ruleInfo;
    for (int i = 0; i < 5; i++) {
      Thread.sleep(1000);
      info = ruleManager.getRuleInfo(id);
      System.out.println(info);
    }

    Assert.assertTrue(info.getCountConditionChecked()
        - ruleInfo.getCountConditionChecked() > 3);
  }

  @Test
  public void testSubmitDeletedRule() throws Exception {
    String rule = "file: every 1s \n | length > 300 | cachefile";
    try {
      long id = ruleManager.submitRule(rule, RuleState.DELETED);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid initState"));
    }
  }

  @Test
  public void testSubmitNewDisabledRule() throws Exception {
    String rule = "file: every 1s \n | length > 300 | cachefile";
    long id = ruleManager.submitRule(rule, RuleState.DISABLED);
    RuleInfo ruleInfo = ruleManager.getRuleInfo(id);
    Assert.assertTrue(ruleInfo.getRuleText().equals(rule));
    RuleInfo info = ruleInfo;
    for (int i = 0; i < 5; i++) {
      Thread.sleep(1000);
      info = ruleManager.getRuleInfo(id);
      System.out.println(info);
    }

    Assert.assertTrue(info.getCountConditionChecked()
        - ruleInfo.getCountConditionChecked() == 0);
  }

  @Test
  public void testSubmitAutoEndsRule() throws Exception {
    String rule = "file: every 1s from now to now + 2s \n | "
        + "length > 300 | cachefile";

    long id = ruleManager.submitRule(rule, RuleState.ACTIVE);
    RuleInfo ruleInfo = ruleManager.getRuleInfo(id);
    Assert.assertTrue(ruleInfo.getRuleText().equals(rule));
    RuleInfo info = ruleInfo;
    for (int i = 0; i < 5; i++) {
      Thread.sleep(1000);
      info = ruleManager.getRuleInfo(id);
      System.out.println(info);
    }

    Assert.assertTrue(info.getState() == RuleState.FINISHED);
    Assert.assertTrue(info.getCountConditionChecked()
        - ruleInfo.getCountConditionChecked() <= 3);
  }

  @Test
  public void testStopRule() throws Exception {
    String rule = "file: every 1s from now to now + 100s \n | "
        + "length > 300 | cachefile";

    long id = ruleManager.submitRule(rule, RuleState.ACTIVE);
    RuleInfo ruleInfo = ruleManager.getRuleInfo(id);
    Assert.assertTrue(ruleInfo.getRuleText().equals(rule));
    RuleInfo info = ruleInfo;
    for (int i = 0; i < 2; i++) {
      Thread.sleep(1000);
      info = ruleManager.getRuleInfo(id);
      System.out.println(info);
    }

    ruleManager.DeleteRule(ruleInfo.getId(), true);
    Thread.sleep(3000);

    RuleInfo endInfo = ruleManager.getRuleInfo(info.getId());
    System.out.println(endInfo);

    Assert.assertTrue(endInfo.getState() == RuleState.DELETED);
    Assert.assertTrue(endInfo.getCountConditionChecked()
        - info.getCountConditionChecked() <= 1);
  }

  @Test
  public void testResumeRule() throws Exception {
    String rule = "file: every 1s from now to now + 100s \n | "
        + "length > 300 | cachefile";

    long id = ruleManager.submitRule(rule, RuleState.ACTIVE);
    RuleInfo ruleInfo = ruleManager.getRuleInfo(id);
    Assert.assertTrue(ruleInfo.getRuleText().equals(rule));
    RuleInfo info = ruleInfo;
    for (int i = 0; i < 2; i++) {
      Thread.sleep(1000);
      info = ruleManager.getRuleInfo(id);
      System.out.println(info);
    }
    Assert.assertTrue(info.getCountConditionChecked()
        > ruleInfo.getCountConditionChecked());

    ruleManager.DisableRule(ruleInfo.getId(), true);
    Thread.sleep(1000);
    RuleInfo info2 = ruleManager.getRuleInfo(id);
    for (int i = 0; i < 3; i++) {
      Thread.sleep(1000);
      info = ruleManager.getRuleInfo(id);
      System.out.println(info);
    }
    Assert.assertTrue(info.getCountConditionChecked()
        == info2.getCountConditionChecked());

    RuleInfo info3 = info;
    ruleManager.ActivateRule(ruleInfo.getId());
    for (int i = 0; i < 3; i++) {
      Thread.sleep(1000);
      info = ruleManager.getRuleInfo(id);
      System.out.println(info);
    }
    Assert.assertTrue(info.getCountConditionChecked()
        > info3.getCountConditionChecked());
  }

  @Test
  public void testSubmitNewMultiRules() throws Exception {
    String rule = "file: every 1s \n | length > 300 | cachefile";

    // id increasing
    int nRules =  3;
    long[] ids = new long[nRules];
    for (int i = 0; i < nRules; i++) {
      ids[i] = ruleManager.submitRule(rule, RuleState.DISABLED);
      System.out.println(ruleManager.getRuleInfo(ids[i]));
      if (i > 0) {
        Assert.assertTrue(ids[i] - ids[i - 1] == 1);
      }
    }

    for (int i = 0; i < nRules; i++) {
      ruleManager.DeleteRule(ids[i], true);
      RuleInfo info = ruleManager.getRuleInfo(ids[i]);
      Assert.assertTrue(info.getState() == RuleState.DELETED);
    }

    long[] ids2 = new long[nRules];
    for (int i = 0; i < nRules; i++) {
      ids2[i] = ruleManager.submitRule(rule, RuleState.DISABLED);
      System.out.println(ruleManager.getRuleInfo(ids2[i]));
      if (i > 0) {
        Assert.assertTrue(ids2[i] - ids2[i - 1] == 1);
      }
      Assert.assertTrue(ids2[i] > ids[nRules - 1]);
    }

    System.out.println("\nFinal state:");
    List<RuleInfo> allRules = ruleManager.getRuleInfo();
    Assert.assertTrue(allRules.size() == 2 * nRules);
    for (RuleInfo info : allRules) {
      System.out.println(info);
    }
  }
}
