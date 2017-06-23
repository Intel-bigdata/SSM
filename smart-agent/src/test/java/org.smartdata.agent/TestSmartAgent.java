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
package org.smartdata.agent;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import com.typesafe.config.Config;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.engine.cmdlet.agent.ActorSystemHarness;
import org.smartdata.server.engine.cmdlet.agent.AgentConstants;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster.RegisterNewAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent.AgentRegistered;
import org.smartdata.server.engine.cmdlet.agent.AgentUtils;

public class TestSmartAgent extends ActorSystemHarness {

  @Test
  public void testAgent() {
    ActorSystem system = getActorSystem();
    JavaTestKit mockedMaster = new JavaTestKit(system);
    SmartConf conf = new SmartConf();
    AgentRunner runner = new AgentRunner(
        AgentUtils.loadConfigWithAddress(conf.get(AgentConstants.AGENT_ADDRESS_KEY)),
        AgentUtils.getFullPath(system, mockedMaster.getRef().path()));
    runner.start();

    mockedMaster.expectMsgClass(RegisterNewAgent.class);
    mockedMaster.reply(new AgentRegistered(new MasterToAgent.AgentId(0)));
  }

  private class AgentRunner extends Thread {

    private final Config config;
    private final String masterPath;

    public AgentRunner(Config config, String masterPath) {
      this.config = config;
      this.masterPath = masterPath;
    }

    @Override
    public void run() {
      SmartAgent agent = new SmartAgent();
      agent.start(config, masterPath);
    }

  }

}
