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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.agent.messages.AgentToMaster.RegisterAgent;
import org.smartdata.agent.messages.AgentToMaster.RegisterNewAgent;
import org.smartdata.agent.messages.MasterToAgent.AgentRegistered;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AgentMaster {

  private static final Logger LOG = LoggerFactory.getLogger(AgentMaster.class);

  public void start() {
    Config config = loadConfig();
    final ActorSystem system = ActorSystem.apply(AgentConstants.MASTER_ACTOR_SYSTEM_NAME, config);

    final ActorRef master = system.actorOf(Props.create(MasterActor.class),
        AgentConstants.MASTER_ACTOR_NAME);
    LOG.info("MasterActor created at {}", AgentUtils.getFullPath(system, master.path()));

    ActorSystemWatcher watcher = new ActorSystemWatcher(system, master);
    watcher.start();
  }

  private Config loadConfig() {
    Config config = ConfigFactory.load();
    AgentUtils.HostPort hostPort = new AgentUtils.HostPort(config.getString(AgentConstants.MASTER_ADDRESS));
    return config.withValue("akka.remote.netty.tcp.host",
        ConfigValueFactory.fromAnyRef(hostPort.getHost()))
        .withValue("akka.remote.netty.tcp.port",
            ConfigValueFactory.fromAnyRef(hostPort.getPort()));
  }

  class ActorSystemWatcher extends Thread {

    private final ActorSystem system;
    private final ActorRef master;

    public ActorSystemWatcher(ActorSystem system, ActorRef master) {
      this.system = system;
      this.master = master;
    }

    @Override
    public void run() {
      final Thread currentThread = Thread.currentThread();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          LOG.info("Shutting down master {}...", AgentUtils.getFullPath(system, master.path()));
          system.stop(master);
          LOG.info("Shutting down system {}...", AgentUtils.getSystemAddres(system));
          system.shutdown();
          try {
            currentThread.join();
          } catch (InterruptedException e) {
            // Ignore
          }
        }
      });
      system.awaitTermination();
    }
  }

  static class MasterActor extends UntypedActor {

    private int nextAgentId = 0;
    private final Map<ActorRef, SmartAgent.AgentId> agentIds = new HashMap<>();
    private final List<ActorRef> agents = new LinkedList<>();

    @Override
    public void onReceive(Object message) throws Exception {
      Boolean handled = handleWorkerMessage(message) ||
          handleTerminatedMessage(message);
      if (!handled) {
        unhandled(message);
      }
    }

    private boolean handleWorkerMessage(Object message) {
      if (message instanceof RegisterNewAgent) {
        SmartAgent.AgentId id = new SmartAgent.AgentId(nextAgentId);
        nextAgentId++;
        getSelf().forward(new RegisterAgent(id), getContext());
        return true;
      } else if (message instanceof RegisterAgent) {
        RegisterAgent register = (RegisterAgent) message;
        ActorRef sender = getSender();
        getContext().watch(sender);
        SmartAgent.AgentId id = register.getId();
        agentIds.put(sender, id);
        agents.add(sender);
        sender.tell(new AgentRegistered(id), getSelf());
        LOG.info("Register agent {} from {}", id, sender);
        return true;
      } else {
        return false;
      }
    }

    private boolean handleTerminatedMessage(Object message) {
      if (message instanceof Terminated) {
        Terminated terminated = (Terminated) message;
        ActorRef ref = terminated.actor();
        SmartAgent.AgentId id = agentIds.remove(ref);
        agents.remove(ref);
        LOG.warn("SmartAgent ({} {} down", id, ref);
        return true;
      } else {
        return false;
      }
    }
  }
}
