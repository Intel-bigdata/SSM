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
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.agent.messages.AgentToMaster.RegisterAgent;
import org.smartdata.agent.messages.AgentToMaster.RegisterNewAgent;
import org.smartdata.agent.messages.MasterToAgent.AgentRegistered;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.cmdlet.message.StatusMessage;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AgentMaster {

  private static final Logger LOG = LoggerFactory.getLogger(AgentMaster.class);
  private static final Timeout TIMEOUT = new Timeout(Duration.create(5, TimeUnit.SECONDS));

  private final ActorSystem system;
  private final ActorRef master;

  public AgentMaster(CmdletManager statusUpdater) {

    Config config = loadConfig();
    system = ActorSystem.apply(AgentConstants.MASTER_ACTOR_SYSTEM_NAME, config);

    master = system.actorOf(Props.create(MasterActor.class, statusUpdater),
        AgentConstants.MASTER_ACTOR_NAME);
    LOG.info("MasterActor created at {}", AgentUtils.getFullPath(system, master.path()));

    ActorSystemWatcher watcher = new ActorSystemWatcher(system, master);
    watcher.start();
  }

  public boolean canAcceptMore() {
    try {
      return (boolean) askMaster(CanAcceptMore.getInstance());
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  public void launchCmdlet(LaunchCmdlet launch) {
    try {
      askMaster(launch);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  private Object askMaster(Object message) throws Exception {
    Future<Object> answer = Patterns.ask(master, message, TIMEOUT);
    return Await.result(answer, TIMEOUT.duration());
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

    private final Map<ActorRef, SmartAgent.AgentId> agents = new HashMap<>();
    private List<ActorRef> resources = new ArrayList<>();
    private int nextAgentId = 0;
    private int dispatchIndex = 0;
    private CmdletManager statusUpdater;

    public MasterActor(CmdletManager statusUpdater) {
      this.statusUpdater = statusUpdater;
    }

    @Override
    public void onReceive(Object message) throws Exception {
      Boolean handled = handleAgentMessage(message) ||
          handleClientMessage(message) ||
          handleTerminatedMessage(message);
      if (!handled) {
        unhandled(message);
      }
    }

    private boolean handleAgentMessage(Object message) {
      if (message instanceof RegisterNewAgent) {
        SmartAgent.AgentId id = new SmartAgent.AgentId(nextAgentId);
        nextAgentId++;
        getSelf().forward(new RegisterAgent(id), getContext());
        return true;
      } else if (message instanceof RegisterAgent) {
        RegisterAgent register = (RegisterAgent) message;
        ActorRef agent = getSender();
        getContext().watch(agent);
        SmartAgent.AgentId id = register.getId();
        agents.put(agent, id);
        AgentRegistered registered = new AgentRegistered(id);
        resources.add(agent);
        agent.tell(registered, getSelf());
        LOG.info("Register SmartAgent {} from {}", id, agent);
        return true;
      } else if (message instanceof StatusMessage) {
        statusUpdater.updateStatue((StatusMessage) message);
        return true;
      } else {
        return false;
      }
    }

    private boolean handleClientMessage(Object message) {
      if (message instanceof CanAcceptMore) {
        if (agents.size() > 0) {
          getSender().tell(true, getSelf());
        } else {
          getSender().tell(false, getSelf());
        }
        return true;
      } else if (message instanceof LaunchCmdlet) {
        getFreeAgent().tell(message, getSelf());
        return true;
      } else {
        return false;
      }
    }

    private boolean handleTerminatedMessage(Object message) {
      if (message instanceof Terminated) {
        Terminated terminated = (Terminated) message;
        ActorRef agent = terminated.actor();
        SmartAgent.AgentId id = agents.remove(agent);
        resources.remove(agent);
        LOG.warn("SmartAgent ({} {} down", id, agent);
        return true;
      } else {
        return false;
      }
    }

    private ActorRef getFreeAgent() {
      int id = dispatchIndex % resources.size();
      dispatchIndex = (id + 1) % resources.size();
      return resources.get(id);
    }
  }

  static class CanAcceptMore implements Serializable {

    private static final CanAcceptMore instance = new CanAcceptMore();

    private CanAcceptMore() {}

    public static CanAcceptMore getInstance() {
      return instance;
    }
  }
}
