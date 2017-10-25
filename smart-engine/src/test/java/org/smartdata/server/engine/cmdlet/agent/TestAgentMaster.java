/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.server.engine.cmdlet.agent;

import org.junit.Test;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestAgentMaster {

  @Test
  public void testAgentMaster() throws Exception {
    CmdletManager statusUpdater = mock(CmdletManager.class);
    AgentMaster master = new AgentMaster(statusUpdater);

    // Wait for master to start
    while (master.getMasterActor() == null) {
      // Do nothing
    }

    Object answer = master.askMaster(AgentToMaster.RegisterNewAgent.getInstance());
    assertTrue(answer instanceof MasterToAgent.AgentRegistered);
    MasterToAgent.AgentRegistered registered = (MasterToAgent.AgentRegistered) answer;
    assertEquals(0, registered.getAgentId().getId());
  }
}
