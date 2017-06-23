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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster.RegisterAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster.RegisterNewAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent.AgentRegistered;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.common.message.StatusMessage;
import org.smartdata.server.engine.cmdlet.message.StopCmdlet;
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
  public static final Timeout TIMEOUT = new Timeout(Duration.create(5, TimeUnit.SECONDS));

  private ActorSystem system;
  private ActorRef master;
  private ResourceManager resourceManager;

  public AgentMaster(CmdletManager statusUpdater) {
    this(new SmartConf(), statusUpdater);
  }

  public AgentMaster(SmartConf conf, CmdletManager statusUpdater) {
    Config config = AgentUtils.loadConfigWithAddress(
        conf.get(AgentConstants.AGENT_MASTER_ADDRESS_KEY));
    this.resourceManager = new ResourceManager();
    Props props = Props.create(MasterActor.class, statusUpdater, resourceManager);
    ActorSystemLauncher launcher = new ActorSystemLauncher(config, props);
    launcher.start();
  }

  public boolean canAcceptMore() {
    return resourceManager.hasFreeResource();
  }

  public void launchCmdlet(LaunchCmdlet launch) {
    try {
      askMaster(launch);
    } catch (Exception e) {
      LOG.error("Failed to launch Cmdlet {} due to {}", launch, e.getMessage());
    }
  }

  public void stopCmdlet(long cmdletId) {
    try {
      askMaster(new StopCmdlet(cmdletId));
    } catch (Exception e) {
      LOG.error("Failed to stop Cmdlet {} due to {}", cmdletId, e.getMessage());
    }
  }

  public void shutdown() {
    if (system != null && !system.isTerminated()) {
      if (master != null && !master.isTerminated()) {
        LOG.info("Shutting down master {}...", AgentUtils.getFullPath(system, master.path()));
        system.stop(master);
      }

      LOG.info("Shutting down system {}...", AgentUtils.getSystemAddres(system));
      system.shutdown();
    }
  }

  Object askMaster(Object message) throws Exception {
    Future<Object> answer = Patterns.ask(master, message, TIMEOUT);
    return Await.result(answer, TIMEOUT.duration());
  }

  class ActorSystemLauncher extends Thread {

    private final Props masterProps;
    private final Config config;

    public ActorSystemLauncher(Config config, Props masterProps) {
      this.config = config;
      this.masterProps = masterProps;
    }

    @Override
    public void run() {
      system = ActorSystem.apply(AgentConstants.MASTER_ACTOR_SYSTEM_NAME, config);

      master = system.actorOf(masterProps, AgentConstants.MASTER_ACTOR_NAME);
      LOG.info("MasterActor created at {}", AgentUtils.getFullPath(system, master.path()));
      final Thread currentThread = Thread.currentThread();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          shutdown();
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

    private final Map<ActorRef, MasterToAgent.AgentId> agents = new HashMap<>();
    private final Map<Long, ActorRef> dispatches = new HashMap<>();
    private int nextAgentId = 0;

    private CmdletManager statusUpdater;
    private ResourceManager resourceManager;

    public MasterActor(CmdletManager statusUpdater, ResourceManager resourceManager) {
      this.statusUpdater = statusUpdater;
      this.resourceManager = resourceManager;
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
        MasterToAgent.AgentId id = new MasterToAgent.AgentId(nextAgentId);
        nextAgentId++;
        getSelf().forward(new RegisterAgent(id), getContext());
        return true;
      } else if (message instanceof RegisterAgent) {
        RegisterAgent register = (RegisterAgent) message;
        ActorRef agent = getSender();
        getContext().watch(agent);
        MasterToAgent.AgentId id = register.getId();
        agents.put(agent, id);
        AgentRegistered registered = new AgentRegistered(id);
        this.resourceManager.addResource(agent);
        agent.tell(registered, getSelf());
        LOG.info("Register SmartAgent {} from {}", id, agent);
        return true;
      } else if (message instanceof StatusMessage) {
        this.statusUpdater.updateStatus((StatusMessage) message);
        return true;
      } else {
        return false;
      }
    }

    private boolean handleClientMessage(Object message) {
      if (message instanceof CanAcceptMore) {
        if (agents.isEmpty()) {
          getSender().tell(false, getSelf());
        } else {
          getSender().tell(true, getSelf());
        }
        return true;
      } else if (message instanceof LaunchCmdlet) {
        if (!agents.isEmpty()) {
          LaunchCmdlet launch = (LaunchCmdlet) message;
          ActorRef agent = this.resourceManager.getFreeResource();
          agent.tell(launch, getSelf());
          dispatches.put(launch.getCmdletId(), agent);
          getSender().tell("Succeed", getSelf());
        }
        return true;
      } else if (message instanceof StopCmdlet) {
        long cmdletId = ((StopCmdlet) message).getCmdletId();
        dispatches.get(cmdletId).tell(message, getSelf());
        getSender().tell("Succeed", getSelf());
        return true;
      } else {
        return false;
      }
    }

    private boolean handleTerminatedMessage(Object message) {
      if (message instanceof Terminated) {
        Terminated terminated = (Terminated) message;
        ActorRef agent = terminated.actor();
        MasterToAgent.AgentId id = agents.remove(agent);
        this.resourceManager.removeResource(agent);
        LOG.warn("SmartAgent ({} {} down", id, agent);
        return true;
      } else {
        return false;
      }
    }

  }

  static class ResourceManager {

    private List<ActorRef> resources = new ArrayList<>();
    private int dispatchIndex = 0;

    void addResource(ActorRef agent) {
      resources.add(agent);
    }

    void removeResource(ActorRef agent) {
      resources.remove(agent);
    }

    boolean hasFreeResource() {
      return !resources.isEmpty();
    }

    ActorRef getFreeResource() {
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
