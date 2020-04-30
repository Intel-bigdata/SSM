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
package org.smartdata.server.engine.cmdlet.agent;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.remote.AssociationEvent;
import akka.remote.DisassociatedEvent;
import akka.util.Timeout;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.protocol.message.LaunchCmdlet;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StopCmdlet;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.CmdletDispatcherHelper;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster.RegisterAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster.RegisterNewAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent.AgentId;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent.AgentRegistered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class AgentMaster {

  private static final Logger LOG = LoggerFactory.getLogger(AgentMaster.class);
  public static final Timeout TIMEOUT =
      new Timeout(Duration.create(5, TimeUnit.SECONDS));

  private ActorSystem system;
  private ActorRef master;
  private AgentManager agentManager;

  private static CmdletManager statusUpdater;
  private static AgentMaster agentMaster = null;

  private AgentMaster(SmartConf conf) throws IOException {
    String[] addresses = AgentUtils.getMasterAddress(conf);
    if (addresses == null) {
      throw new IOException("AgentMaster address not configured!");
    }
    String address = addresses[0];
    LOG.info("Agent master: " + address);
    Config config = AgentUtils.overrideRemoteAddress(
        ConfigFactory.load(AgentConstants.AKKA_CONF_FILE), address);
    CmdletDispatcherHelper.init();
    this.agentManager = new AgentManager();
    Props props = Props.create(MasterActor.class, null, agentManager);
    ActorSystemLauncher launcher = new ActorSystemLauncher(config, props);
    launcher.start();
  }

  public static AgentMaster getAgentMaster() throws IOException {
    return getAgentMaster(new SmartConf());
  }

  public static AgentMaster getAgentMaster(SmartConf conf)
      throws IOException {
    if (agentMaster == null) {
      agentMaster = new AgentMaster(conf);
      return agentMaster;
    } else {
      return agentMaster;
    }
  }

  public static void setCmdletManager(CmdletManager statusUpdater) {
    AgentMaster.statusUpdater = statusUpdater;
  }

  public boolean canAcceptMore() {
    return agentManager.hasFreeAgent();
  }

  public String launchCmdlet(LaunchCmdlet launch) {
    try {
      AgentId agentId = (AgentId) askMaster(launch);
      return agentId.getId();
    } catch (Exception e) {
      LOG.error(
          "Failed to launch Cmdlet {} due to {}", launch, e.getMessage());
      return null;
    }
  }

  public void stopCmdlet(long cmdletId) {
    try {
      askMaster(new StopCmdlet(cmdletId));
    } catch (Exception e) {
      LOG.error(
          "Failed to stop Cmdlet {} due to {}", cmdletId, e.getMessage());
    }
  }

  public void shutdown() {
    if (system != null && !system.isTerminated()) {
      if (master != null && !master.isTerminated()) {
        LOG.info("Shutting down master {}...",
            AgentUtils.getFullPath(system, master.path()));
        system.stop(master);
      }

      LOG.info("Shutting down system {}...",
          AgentUtils.getSystemAddres(system));
      system.shutdown();
    }
  }

  public List<AgentInfo> getAgentInfos() {
    List<AgentInfo> infos = new ArrayList<>();
    for (Map.Entry<ActorRef, AgentId> entry :
        agentManager.getAgents().entrySet()) {
      String location = AgentUtils.getHostPort(entry.getKey());
      infos.add(new AgentInfo(String.valueOf(
          entry.getValue().getId()), location));
    }
    return infos;
  }

  public int getNumAgents() {
    return agentManager.getAgents().size();
  }

  @VisibleForTesting
  ActorRef getMasterActor() {
    return master;
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
      system = ActorSystem.apply(
          AgentConstants.MASTER_ACTOR_SYSTEM_NAME, config);

      master = system.actorOf(masterProps, AgentConstants.MASTER_ACTOR_NAME);
      LOG.info("MasterActor created at {}",
          AgentUtils.getFullPath(system, master.path()));
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
    private final Map<Long, ActorRef> dispatches = new HashMap<>();
    private AgentManager agentManager;

    public MasterActor(CmdletManager statusUpdater,
        AgentManager agentManager) {
      this(agentManager);
      if (statusUpdater != null) {
        setCmdletManager(statusUpdater);
      }
    }

    public MasterActor(AgentManager agentManager) {
      this.agentManager = agentManager;
    }

    /**
     * Subscribe an event: {@code DisassociatedEvent}. It will be
     * handled by {@link #handleDisassociatedEvent method}.
     */
    @Override
    public void preStart() {
      this.context().system().eventStream().subscribe(
          self(), DisassociatedEvent.class);
    }

    @Override
    public void onReceive(Object message) throws Exception {
      Boolean handled =
          handleAgentMessage(message)
              || handleClientMessage(message)
              || handleTerminatedMessage(message)
              || handleDisassociatedEvent(message);
      if (!handled) {
        unhandled(message);
      }
    }

    private boolean handleAgentMessage(Object message) {
      if (message instanceof RegisterNewAgent) {
        getSelf().forward(new RegisterAgent(
            ((RegisterNewAgent) message).getId()), getContext());
        return true;
      } else if (message instanceof RegisterAgent) {
        RegisterAgent register = (RegisterAgent) message;
        ActorRef agent = getSender();
        // Watch this agent to listen messages delivered from it.
        getContext().watch(agent);
        AgentId id = register.getId();
        AgentRegistered registered = new AgentRegistered(id);
        this.agentManager.addAgent(agent, id);
        agent.tell(registered, getSelf());
        LOG.info("Register SmartAgent {} from {}", id, agent);
        return true;
      } else if (message instanceof StatusMessage) {
        AgentMaster.statusUpdater.updateStatus((StatusMessage) message);
        return true;
      } else {
        return false;
      }
    }

    private boolean handleClientMessage(Object message) {
      if (message instanceof LaunchCmdlet) {
        if (agentManager.hasFreeAgent()) {
          LaunchCmdlet launch = (LaunchCmdlet) message;
          ActorRef agent = this.agentManager.dispatch(launch.getNodeId());
          AgentId agentId = this.agentManager.getAgentId(agent);
          agent.tell(launch, getSelf());
          dispatches.put(launch.getCmdletId(), agent);
          getSender().tell(agentId, getSelf());
        }
        return true;
      } else if (message instanceof StopCmdlet) {
        long cmdletId = ((StopCmdlet) message).getCmdletId();
        if (dispatches.containsKey(cmdletId)) {
          dispatches.get(cmdletId).tell(message, getSelf());
          getSender().tell("Succeed", getSelf());
        } else {
          getSender().tell("NotFound", getSelf());
        }
        return true;
      } else {
        return false;
      }
    }

    private boolean handleTerminatedMessage(Object message) {
      if (message instanceof Terminated) {
        Terminated terminated = (Terminated) message;
        ActorRef agent = terminated.actor();
        AgentId id = this.agentManager.removeAgent(agent);
        // Unwatch this agent to avoid trying re-association.
        this.context().unwatch(agent);
        LOG.warn("SmartAgent ({} {} down", id, agent);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Remove agent if {@code DisassociatedEvent} is received.
     */
    private boolean handleDisassociatedEvent(Object message) {
      if (!(message instanceof DisassociatedEvent)) {
        return false;
      }
      AssociationEvent associEvent = (AssociationEvent) message;
      ActorRef agent = agentManager.getAgentActorByAddress(
          associEvent.getRemoteAddress());
      // The agent may be already removed. Return true to indicate
      // the message has been handled.
      if (agent == null) {
        return true;
      }
      LOG.warn("Received event: {}, details: {}",
          associEvent.eventName(), associEvent.toString());
      LOG.warn("Removing the disassociated agent: " + agent.path().address());
      agentManager.removeAgent(agent);
      // Unwatch this agent to avoid trying re-association.
      this.context().unwatch(agent);
      return true;
    }
  }
}
