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
import akka.actor.Cancellable;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.japi.Procedure;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.actions.ActionFactory;
import org.smartdata.actions.ActionRegistry;
import org.smartdata.actions.SmartAction;
import org.smartdata.agent.messages.AgentToMaster.RegisterNewAgent;
import org.smartdata.agent.messages.MasterToAgent.AgentRegistered;
import org.smartdata.server.cmdlet.CmdletFactory;
import org.smartdata.server.cmdlet.executor.CmdletExecutor;
import org.smartdata.server.cmdlet.executor.CmdletStatusReporter;
import org.smartdata.server.cmdlet.message.LaunchAction;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.cmdlet.message.StatusMessage;
import scala.Serializable;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class SmartAgent {
  private static final String NAME = "SmartAgent";
  static final String MASTER_PATH = "master.path";
  private final static Logger LOG = LoggerFactory.getLogger(SmartAgent.class);
  private ActorSystem system;

  public static void main(String[] args) {
    SmartAgent agent = new SmartAgent();

    Config config = ConfigFactory.load();
    String masterAddress = config.getString(AgentConstants.MASTER_ADDRESS);
    agent.start(config.withValue(MASTER_PATH,
        ConfigValueFactory.fromAnyRef(AgentUtils.getMasterActorPath(masterAddress))));
  }

  void start(Config config) {
    system = ActorSystem.apply(NAME, config);
    system.actorOf(Props.create(AgentActor.class, this, config.getString(MASTER_PATH)));
    system.awaitTermination();
  }

  void close() {
    if (system != null && !system.isTerminated()) {
      LOG.info("Shutting down system {}", AgentUtils.getSystemAddres(system));
      system.shutdown();
    }
  }

  static class AgentActor extends UntypedActor implements CmdletStatusReporter {
    private final static Logger LOG = LoggerFactory.getLogger(AgentActor.class);

    private final static FiniteDuration TIMEOUT = Duration.create(30, TimeUnit.SECONDS);
    private final static FiniteDuration RETRY_INTERVAL = Duration.create(2, TimeUnit.SECONDS);

    private AgentId id;
    private ActorRef master;
    private final SmartAgent agent;
    private final String masterPath;
    private final CmdletExecutor executor;
    private final CmdletFactory factory;

    public AgentActor(SmartAgent agent, String masterPath) {
      this.agent = agent;
      this.masterPath = masterPath;
      this.executor = new CmdletExecutor(this);
      this.factory = new CmdletFactory(new SmartContext());
    }

    @Override
    public void onReceive(Object message) throws Exception {
      unhandled(message);
    }

    @Override
    public void preStart() {
      Cancellable findMaster = findMaster(masterPath);
      getContext().become(new WaitForFindMaster(findMaster));
    }

    private Cancellable findMaster(final String masterPath) {
      final ActorSelection actorSelection = getContext().actorSelection(masterPath);
      return AgentUtils.repeatActionUntil(getContext().system(),
          Duration.Zero(), RETRY_INTERVAL, TIMEOUT,
          new Runnable() {
            @Override
            public void run() {
              actorSelection.tell(new Identify(null), getSelf());
            }
          },
          new Shutdown(agent));
    }

    @Override
    public void report(StatusMessage status) {
      master.tell(status, getSelf());
    }

    private class WaitForFindMaster implements Procedure<Object> {

      private final Cancellable findMaster;

      public WaitForFindMaster(Cancellable findMaster) {
        this.findMaster = findMaster;
      }

      @Override
      public void apply(Object message) throws Exception {
        if (message instanceof ActorIdentity) {
          ActorIdentity identity = (ActorIdentity) message;
          master = identity.getRef();
          if (master != null) {
            findMaster.cancel();
            Cancellable registerAgent =
                AgentUtils.repeatActionUntil(getContext().system(), Duration.Zero(),
                    RETRY_INTERVAL, TIMEOUT,
                    new SendMessage(master, RegisterNewAgent.getInstance()), new Shutdown(agent));
            LOG.info("Registering to master {}", master);
            getContext().become(new WaitForRegisterAgent(registerAgent));
          }
        }
      }
    }

    private class WaitForRegisterAgent implements Procedure<Object> {

      private final Cancellable registerAgent;

      public WaitForRegisterAgent(Cancellable registerAgent) {
        this.registerAgent = registerAgent;
      }

      @Override
      public void apply(Object message) throws Exception {
        if (message instanceof AgentRegistered) {
          AgentRegistered registered = (AgentRegistered) message;
          registerAgent.cancel();
          getContext().watch(master);
          AgentActor.this.id = registered.getAgentId();
          LOG.info("SmartAgent {} registered to {}",
              AgentActor.this.id,
              AgentUtils.getFullPath(getContext().system(), getSelf().path()));
          getContext().become(new Serve());
        }
      }
    }

    private class Serve implements Procedure<Object> {

      @Override
      public void apply(Object message) throws Exception {
        if (message instanceof LaunchCmdlet) {
          LaunchCmdlet launch = (LaunchCmdlet) message;
          executor.execute(factory.createCmdlet(launch));
        } else if (message instanceof Terminated) {
          Terminated terminated = (Terminated) message;
          if (terminated.getActor().equals(master)) {
            LOG.warn("Lost contact with master {}. Try registering again...", getSender());
            getContext().become(new WaitForFindMaster(findMaster(masterPath)));
          }
        }
      }
    }

    private class SendMessage implements Runnable {

      private final ActorRef to;
      private final Object message;

      public SendMessage(ActorRef to, Object message) {
        this.to = to;
        this.message = message;
      }

      @Override
      public void run() {
        to.tell(message, getSelf());
      }
    }

    private class Shutdown implements Runnable {

      private SmartAgent agent;

      public Shutdown(SmartAgent agent) {
        this.agent = agent;
      }

      @Override
      public void run() {
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
        LOG.info("Failed to find master after {}; Shutting down...", TIMEOUT);
        agent.close();
      }
    }
  }

  public static class AgentId implements Serializable {

    private static final long serialVersionUID = -4032231012646281770L;
    private final int id;

    public AgentId(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AgentId agentId = (AgentId) o;

      return id == agentId.id;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public String toString() {
      return "AgentId{" +
          "id=" + id +
          '}';
    }  }
}
