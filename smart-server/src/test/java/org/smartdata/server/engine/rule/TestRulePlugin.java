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
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.model.rule.RulePlugin;
import org.smartdata.model.rule.RulePluginManager;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.server.MiniSmartClusterHarness;

import java.io.IOException;

public class TestRulePlugin extends MiniSmartClusterHarness {

  @Test
  public void testPlugin() throws Exception {
    waitTillSSMExitSafeMode();

    SimplePlugin plugin = new SimplePlugin();
    try {
      RulePluginManager.addPlugin(plugin);
      int adding = plugin.getAdding();
      int added = plugin.getAdded();

      SmartAdmin admin = new SmartAdmin(smartContext.getConf());
      admin.submitRule("file : path matches \"/home/*\" | cache", RuleState.ACTIVE);
      Assert.assertTrue(adding + 1 == plugin.getAdding());
      Assert.assertTrue(added + 1 == plugin.getAdded());

      try {
        admin.submitRule("file : path matches \"/user/*\" | cache", RuleState.DISABLED);
        Assert.fail("Should not success.");
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("MUST ACTIVE"));
      }
      Assert.assertTrue(adding + 1 == plugin.getAdding());
      Assert.assertTrue(added + 1 == plugin.getAdded());
    } finally {
      RulePluginManager.deletePlugin(plugin);
    }
  }

  private class SimplePlugin implements RulePlugin {
    private int adding = 0;
    private int added = 0;

    public SimplePlugin() {
    }

    public int getAdding() {
      return adding;
    }

    public int getAdded() {
      return added;
    }

    public void onAddingNewRule(RuleInfo ruleInfo, TranslateResult tr)
        throws IOException {
      if (ruleInfo.getState() == RuleState.ACTIVE) {
        adding++;
      } else {
        throw new IOException("MUST ACTIVE");
      }
    }

    public void onNewRuleAdded(RuleInfo ruleInfo, TranslateResult tr) {
      added++;
    }
  }
}
