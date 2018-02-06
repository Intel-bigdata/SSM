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
package org.smartdata.integration;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.server.engine.CmdletManager;

import java.io.IOException;

public class TestSmartServerHA extends IntegrationTestBase {
  private static IntegrationSmartAgent agent = new IntegrationSmartAgent();

  @BeforeClass
  public static void setupAgent() throws IOException {
    agent.setup();
  }

  public CmdletDescriptor generateCmdletDescriptor(String cmd) throws Exception {
    CmdletDescriptor cmdletDescriptor = new CmdletDescriptor(cmd);
    cmdletDescriptor.setRuleId(1);
    return cmdletDescriptor;
  }

  @Test(timeout = 40000)
  public void TestServerHA() throws Exception {
    CmdletManager cmdletManager = smartServer.getSSM().getCmdletManager();
    cmdletManager.setTimeout(1000);

    String cmd = "sleep -ms 100";
    long cid1 = cmdletManager.submitCmdlet(generateCmdletDescriptor(cmd));
    CmdletInfo cmdletInfo1 = cmdletManager.getCmdletInfo(cid1);
    while (cmdletInfo1.getState() != CmdletState.DONE) {
      Thread.sleep(100);
    }

    cmd = "sleep -ms 100000";
    long cid2 = cmdletManager.submitCmdlet(generateCmdletDescriptor(cmd));
    CmdletInfo cmdletInfo2 = cmdletManager.getCmdletInfo(cid2);
    while (cmdletInfo2.getState() != CmdletState.DISPATCHED) {
      Thread.sleep(100);
    }
    closeAgent();
    while (cmdletInfo2.getState() != CmdletState.FAILED) {
      Thread.sleep(100);
    }
    ActionInfo actionInfo = cmdletManager.getActionInfo(cmdletInfo2.getAids().get(0));
    Assert.assertTrue(actionInfo.getLog().equals(CmdletManager.TIMEOUTLOG));
  }

  @AfterClass
  public static void closeAgent() {
    System.out.println("Shutting down agent..");
    agent.close();
  }
}
