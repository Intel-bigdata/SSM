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
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AgentService;
import org.smartdata.SmartConstants;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReporter;
import org.smartdata.server.engine.cmdlet.CmdletExecutor;
import org.smartdata.server.engine.cmdlet.StatusReportTask;
import org.smartdata.server.engine.cmdlet.agent.AgentCmdletService;
import org.smartdata.server.engine.cmdlet.agent.AgentConstants;
import org.smartdata.server.engine.cmdlet.agent.AgentUtils;
import org.smartdata.server.engine.cmdlet.agent.SmartAgentContext;
import org.smartdata.server.engine.cmdlet.agent.messages.AgentToMaster.RegisterNewAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent.AgentRegistered;
import org.smartdata.server.utils.GenericOptionsParser;
import org.smartdata.utils.SecurityUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

public class SmartAgent implements StatusReporter {
  private static final String NAME = "SmartAgent";
  private static final Logger LOG = LoggerFactory.getLogger(SmartAgent.class);
  private ActorSystem system;
  private ActorRef agentActor;
  private CmdletExecutor cmdletExecutor;

  public static void main(String[] args) throws IOException {
    SmartAgent agent = new SmartAgent();

    SmartConf conf = (SmartConf) new GenericOptionsParser(new SmartConf(), args).getConfiguration();

    String[] masters = AgentUtils.getMasterAddress(conf);
    if (masters == null) {
      throw new IOException("No master address found!");
    }
    for (int i = 0; i < masters.length; i++) {
      LOG.info("Agent master " + i + ":  " + masters[i]);
    }
    String agentAddress = AgentUtils.getAgentAddress(conf);
    LOG.info("Agent address: " + agentAddress);
    RegisterNewAgent.getInstance("SSMAgent@" + agentAddress.replaceAll(":.*$", ""));

    HadoopUtil.setSmartConfByHadoop(conf);
    agent.authentication(conf);

    agent.start(AgentUtils.overrideRemoteAddress(ConfigFactory.load(AgentConstants.AKKA_CONF_FILE),
        agentAddress), AgentUtils.getMasterActorPaths(masters), conf);
  }

  //TODO: remove loadHadoopConf
  private void authentication(SmartConf conf) throws IOException {
    if (!SecurityUtil.isSecurityEnabled(conf)) {
      return;
    }

    // Load Hadoop configuration files
    try {
      HadoopUtil.loadHadoopConf(conf);
    } catch (IOException e) {
      LOG.info("Running in secure mode, but cannot find Hadoop configuration file. "
              + "Please config smart.hadoop.conf.path property in smart-site.xml.");
      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("hadoop.security.authorization", "true");
    }

    UserGroupInformation.setConfiguration(conf);

    String keytabFilename = conf.get(SmartConfKeys.SMART_AGENT_KEYTAB_FILE_KEY);
    String principalConfig = conf.get(SmartConfKeys.SMART_AGENT_KERBEROS_PRINCIPAL_KEY);
    String principal =
        org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principalConfig, (String) null);

    SecurityUtil.loginUsingKeytab(keytabFilename, principal);
  }

  private static String getDateString() {
    Date now = new Date();
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    return df.format(now);
  }

  public void start(Config config, String[] masterPath, SmartConf conf) {
    system = ActorSystem.apply(NAME, config);
    agentActor = system.actorOf(
            Props.create(AgentActor.class, this, masterPath, conf), getAgentName());

    final Thread currentThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        close();
        try {
          currentThread.join();
        } catch (InterruptedException e) {
          // Ignore
        }
      }
    });
    Services.init(new SmartAgentContext(conf, this));
    Services.start();

    AgentCmdletService agentCmdletService =
            (AgentCmdletService) Services.getService(SmartConstants.AGENT_CMDLET_SERVICE_NAME);
    cmdletExecutor = agentCmdletService.getCmdletExecutor();

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    int reportPeriod = conf.getInt(SmartConfKeys.SMART_STATUS_REPORT_PERIOD_KEY,
            SmartConfKeys.SMART_STATUS_REPORT_PERIOD_DEFAULT);
    StatusReportTask statusReportTask = new StatusReportTask(this, cmdletExecutor, conf);
    executorService.scheduleAtFixedRate(
            statusReportTask, 1000, reportPeriod, TimeUnit.MILLISECONDS);

    system.awaitTermination();
  }

  public void close() {
    Services.stop();
    if (system != null && !system.isTerminated()) {
      LOG.info("Shutting down system {}", AgentUtils.getSystemAddres(system));
      system.shutdown();
    }
  }

  @Override
  public void report(StatusMessage status) {
    Patterns.ask(agentActor, status, Timeout.apply(5, TimeUnit.SECONDS));
  }

  private String getAgentName() {
    return "agent-" + UUID.randomUUID().toString();
  }

  static class AgentActor extends UntypedActor {
    private static final Logger LOG = LoggerFactory.getLogger(AgentActor.class);

    private static final FiniteDuration TIMEOUT = Duration.create(30, TimeUnit.SECONDS);
    private static final FiniteDuration RETRY_INTERVAL = Duration.create(2, TimeUnit.SECONDS);

    private MasterToAgent.AgentId id;
    private ActorRef master;
    private final SmartAgent agent;
    private final String[] masters;
    private SmartConf conf;

    public AgentActor(SmartAgent agent, String[] masters, SmartConf conf) {
      this.agent = agent;
      this.masters = masters;
      this.conf =  conf;
    }

    @Override
    public void onReceive(Object message) throws Exception {
      unhandled(message);
    }

    /**
     * Change the context to WaitForFindMaster. Context can be viewed as
     * a kind of agent status. And the transition from one context to
     * another one can occur. Under a specific context, some methods are
     * defined to tell agent what to do.
     * <p></p>
     * 1. After agent starts, the context becomes WaitForFindMaster. Under
     * it, agent try to find agent master (see AgentMaster.java) and defines
     * #apply method to tackle message from master. After master is found,
     * the context becomes WaitForRegisterAgent.
     * <p><p/>
     * 2. In WaitForRegisterAgent context, agent will send RegisterNewAgent
     * message to master. And #apply method in WaitForRegisterAgent will
     * tackle message from master. A unique agent id is contained in the
     * message of master. After the tackling, the context becomes Serve.
     * <p><p/>
     * 3. In Serve context, agent is in normal service to respond to master's
     * request of executing SSM action wrapped in master's message. In this
     * context, if agent loses connection with master, the context will go
     * back to WaitForRegisterAgent. And an agent will go through the above
     * procedure.
     */
    @Override
    public void preStart() {
      Cancellable findMaster = findMaster();
      getContext().become(new WaitForFindMaster(findMaster));
    }

    /**
     * Find master by trying the configured smart servers one by one.
     * The retry interval value and timeout value are specified above.
     */
    private Cancellable findMaster() {
      return AgentUtils.repeatActionUntil(getContext().system(),
          Duration.Zero(), RETRY_INTERVAL, TIMEOUT,
          new Runnable() {
            @Override
            public void run() {
              for (String m : masters) {
                // Pick up one possible master server and send message to it.
                final ActorSelection actorSelection = getContext().actorSelection(m);
                actorSelection.tell(new Identify(null), getSelf());
              }
            }
          }, new Shutdown(agent));
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

            String rpcHost = master.path().address().host().get();
            String rpcPort = conf
                    .get(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY,
                            SmartConfKeys.SMART_SERVER_RPC_ADDRESS_DEFAULT)
                    .split(":")[1];
            conf.set(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY, rpcHost + ":" + rpcPort);

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
        if (message instanceof AgentService.Message) {
          try {
            Services.dispatch((AgentService.Message) message);
          } catch (Exception e) {
            LOG.error(e.getMessage());
          }
        } else if (message instanceof StatusMessage) {
          master.tell(message, getSelf());
          getSender().tell("status reported", getSelf());
        } else if (message instanceof Terminated) {
          Terminated terminated = (Terminated) message;
          if (terminated.getActor().equals(master)) {
            // Go back to WaitForFindMaster context to find new master.
            LOG.warn("Lost contact with master {}. Try registering again...", getSender());
            getContext().become(new WaitForFindMaster(findMaster()));
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
}
