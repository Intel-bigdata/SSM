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
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.RuleExecutorPluginManager;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.server.MiniSmartClusterHarness;

import java.util.List;

public class TestRuleExecutorPlugin extends MiniSmartClusterHarness {

  @Test
  public void testPlugin() throws Exception {
    waitTillSSMExitSafeMode();

    TestPlugin plugin = new TestPlugin();
    try {
      RuleExecutorPluginManager.addPlugin(plugin);
      RuleExecutorPluginManager.addPlugin(plugin);

      String rule = "file: every 1s \n | length > 10 | cache";
      SmartAdmin client = new SmartAdmin(smartContext.getConf());

      long ruleId = client.submitRule(rule, RuleState.ACTIVE);

      Assert.assertEquals(plugin.getNumOnNewRuleExecutor(), 1);
      Thread.sleep(3000);
      Assert.assertEquals(plugin.getNumOnNewRuleExecutor(), 1);
      Assert.assertTrue(plugin.getNumPreExecution() >= 2);
      Assert.assertTrue(plugin.getNumPreSubmitCmdlet() >= 2);
      Assert.assertTrue(plugin.getNumOnRuleExecutorExit() == 0);

      client.disableRule(ruleId, true);
      Thread.sleep(1100);
      int numPreExecution = plugin.getNumPreExecution();
      int numPreSubmitCmdlet = plugin.getNumPreSubmitCmdlet();
      Thread.sleep(2500);
      Assert.assertTrue(plugin.getNumOnNewRuleExecutor() == 1);
      Assert.assertTrue(plugin.getNumPreExecution() == numPreExecution);
      Assert.assertTrue(plugin.getNumPreSubmitCmdlet() == numPreSubmitCmdlet);
      Assert.assertTrue(plugin.getNumOnRuleExecutorExit() == 1);

      RuleExecutorPluginManager.deletePlugin(plugin);
      client.activateRule(ruleId);
      Thread.sleep(500);
      Assert.assertTrue(plugin.getNumOnNewRuleExecutor() == 1);
      Assert.assertTrue(plugin.getNumPreExecution() == numPreExecution);
    } finally {
      RuleExecutorPluginManager.deletePlugin(plugin);
    }
  }

  private class TestPlugin implements RuleExecutorPlugin {
    private volatile int numOnNewRuleExecutor = 0;
    private volatile int numPreExecution = 0;
    private volatile int numPreSubmitCmdlet = 0;
    private volatile int numOnRuleExecutorExit = 0;

    public TestPlugin() {
    }

    public void onNewRuleExecutor(final RuleInfo ruleInfo, TranslateResult tResult) {
      numOnNewRuleExecutor++;
    }

    public boolean preExecution(final RuleInfo ruleInfo, TranslateResult tResult) {
      numPreExecution++;
      return true;
    }

    public List<String> preSubmitCmdlet(final RuleInfo ruleInfo, List<String> objects) {
      numPreSubmitCmdlet++;
      return objects;
    }

    public CmdletDescriptor preSubmitCmdletDescriptor(
        final RuleInfo ruleInfo, TranslateResult tResult, CmdletDescriptor descriptor) {
      return descriptor;
    }

    public void onRuleExecutorExit(final RuleInfo ruleInfo) {
      numOnRuleExecutorExit++;
    }

    public int getNumOnNewRuleExecutor() {
      return numOnNewRuleExecutor;
    }

    public int getNumPreExecution() {
      return numPreExecution;
    }

    public int getNumPreSubmitCmdlet() {
      return numPreSubmitCmdlet;
    }

    public int getNumOnRuleExecutorExit() {
      return numOnRuleExecutorExit;
    }
  }
}
