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
package org.smartdata.server.engine.rule;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.model.RuleState;
import org.smartdata.server.MiniSmartClusterHarness;

import java.io.IOException;

public class TestSubmitRule extends MiniSmartClusterHarness {

  @Test
  public void testSubmitRule() throws Exception {
    waitTillSSMExitSafeMode();

    String rule = "file: every 1s \n | length > 10 | cache";
    SmartAdmin client = new SmartAdmin(smartContext.getConf());

    long ruleId = client.submitRule(rule, RuleState.ACTIVE);

    for (int i = 0; i < 10; i++) {
      long id = client.submitRule(rule, RuleState.ACTIVE);
      Assert.assertTrue(ruleId + i + 1 == id);
    }

    String badRule = "something else";
    try {
      client.submitRule(badRule, RuleState.ACTIVE);
      Assert.fail("Should have an exception here");
    } catch (IOException e) {
    }

    try {
      client.checkRule(badRule);
      Assert.fail("Should have an exception here");
    } catch (IOException e) {
    }
  }
}
