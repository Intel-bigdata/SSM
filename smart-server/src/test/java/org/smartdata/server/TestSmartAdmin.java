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
package org.smartdata.server;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.model.ActionDescriptor;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestSmartAdmin extends MiniSmartClusterHarness {

  @Test
  public void test() throws Exception {
    waitTillSSMExitSafeMode();

    SmartAdmin admin = null;

    try {
      admin = new SmartAdmin(smartContext.getConf());

      //test listRulesInfo and submitRule
      List<RuleInfo> ruleInfos = admin.listRulesInfo();
      int ruleCounts0 = ruleInfos.size();
      long ruleId = admin.submitRule(
          "file: every 5s | path matches \"/foo*\"| cache",
          RuleState.DRYRUN);
      ruleInfos = admin.listRulesInfo();
      int ruleCounts1 = ruleInfos.size();
      assertEquals(1, ruleCounts1 - ruleCounts0);

      //test checkRule
      //if success ,no Exception throw
      admin.checkRule("file: every 5s | path matches \"/foo*\"| cache");
      boolean caughtException = false;
      try {
        admin.checkRule("file.path");
      } catch (IOException e) {
        caughtException = true;
      }
      assertTrue(caughtException);

      //test getRuleInfo
      RuleInfo ruleInfo = admin.getRuleInfo(ruleId);
      assertNotEquals(null, ruleInfo);

      //test disableRule
      admin.disableRule(ruleId, true);
      assertEquals(RuleState.DISABLED, admin.getRuleInfo(ruleId).getState());

      //test activateRule
      admin.activateRule(ruleId);
      assertEquals(RuleState.ACTIVE, admin.getRuleInfo(ruleId).getState());

      //test deleteRule
      admin.deleteRule(ruleId, true);
      assertEquals(RuleState.DELETED, admin.getRuleInfo(ruleId).getState());

      //test cmdletInfo
      long id = admin.submitCmdlet("cache -file /foo*");
      CmdletInfo cmdletInfo = admin.getCmdletInfo(id);
      assertTrue("cache -file /foo*".equals(cmdletInfo.getParameters()));

      //test actioninfo
      List<Long> aidlist = cmdletInfo.getAids();
      assertNotEquals(0, aidlist.size());
      ActionInfo actionInfo = admin.getActionInfo(aidlist.get(0));
      assertEquals(id, actionInfo.getCmdletId());

      //test listActionInfoOfLastActions
      admin.listActionInfoOfLastActions(2);

      List<ActionDescriptor> actions = admin.listActionsSupported();
      assertTrue(actions.size() > 0);

      //test client close
      admin.close();
      try {
        admin.getRuleInfo(ruleId);
        Assert.fail("Should fail because admin has closed.");
      } catch (IOException e) {
      }

      admin = null;
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
}
