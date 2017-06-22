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

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.agent.AgentConstants;
import org.smartdata.server.engine.cmdlet.agent.AgentMaster;
import org.smartdata.server.engine.cmdlet.agent.AgentUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TestAgentMaster extends ActorSystemHarness {

  @Test
  public void testAgentMaster() {
    CmdletManager statusUpdater = mock(CmdletManager.class);
    AgentMaster master = new AgentMaster(statusUpdater);

    ActorSystem system = getActorSystem();
    ActorRef mockedAgent = system.actorOf(Props.create(MockedAgent.class));
    String addr = ConfigFactory.load().getString(AgentConstants.MASTER_ADDRESS);
    ActorSelection selection = system
        .actorSelection(AgentUtils.getMasterActorPath(addr));
    selection.tell(Identify.apply(null), mockedAgent);
  }

  private static class MockedAgent extends UntypedActor {

    private int id = 0;
    private ActorRef master;

    @Override
    public void onReceive(Object message) throws Exception {
      if (message instanceof ActorIdentity) {
        ActorIdentity identity = (ActorIdentity) message;
        master = identity.getRef();
        master.tell(AgentToMaster.RegisterNewAgent.getInstance(), getSelf());
      } else if (message instanceof MasterToAgent.AgentRegistered) {
        MasterToAgent.AgentRegistered registered = (MasterToAgent.AgentRegistered) message;
        assertEquals(id, registered.getAgentId().getId());
        id++;
        if (id < 10) {
          master.tell(AgentToMaster.RegisterNewAgent.getInstance(), getSelf());
        }
      } else {
        fail();
      }

    }
  }
}
