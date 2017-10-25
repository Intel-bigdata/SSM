/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.server.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.action.ActionException;
import org.smartdata.model.CmdletState;
import org.smartdata.protocol.message.ActionStatusReport;
import org.smartdata.protocol.message.CmdletStatusUpdate;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReporter;
import org.smartdata.server.engine.cmdlet.CmdletExecutor;
import org.smartdata.server.engine.cmdlet.CmdletFactory;
import org.smartdata.server.engine.cmdlet.HazelcastExecutorService;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.cmdlet.message.StopCmdlet;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Todo: recover and reconnect when master is offline
public class HazelcastWorker implements StatusReporter {
  private static final Logger LOG = LoggerFactory.getLogger(HazelcastWorker.class);
  private final HazelcastInstance instance;
  private ScheduledExecutorService executorService;
  private ITopic<Serializable> masterMessages;
  private ITopic<StatusMessage> statusTopic;
  private CmdletExecutor cmdletExecutor;
  private CmdletFactory factory;
  private Future<?> fetcher;

  public HazelcastWorker(SmartContext smartContext) {
    this.factory = new CmdletFactory(smartContext, this);
    this.cmdletExecutor = new CmdletExecutor(smartContext.getConf(), this);
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.instance = HazelcastInstanceProvider.getInstance();
    this.statusTopic = instance.getTopic(HazelcastExecutorService.STATUS_TOPIC);
    String instanceId = instance.getCluster().getLocalMember().getUuid();
    this.masterMessages =
        instance.getTopic(HazelcastExecutorService.WORKER_TOPIC_PREFIX + instanceId);
    this.masterMessages.addMessageListener(new MasterMessageListener());
  }

  public void start() {
    fetcher =
        executorService.scheduleAtFixedRate(
            new StatusReporter(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    if (fetcher != null) {
      fetcher.cancel(true);
    }
    executorService.shutdown();
    cmdletExecutor.shutdown();
  }

  @Override
  public void report(StatusMessage status) {
    statusTopic.publish(status);
  }

  private class MasterMessageListener implements MessageListener<Serializable> {
    @Override
    public void onMessage(Message<Serializable> message) {
      Serializable msg = message.getMessageObject();
      if (msg instanceof LaunchCmdlet) {
        LaunchCmdlet launchCmdlet = (LaunchCmdlet) msg;
        try {
          cmdletExecutor.execute(factory.createCmdlet(launchCmdlet));
        } catch (ActionException e) {
          LOG.error("Failed to create cmdlet from {}", launchCmdlet, e);
          report(
              new CmdletStatusUpdate(
                  launchCmdlet.getCmdletId(), System.currentTimeMillis(), CmdletState.FAILED));
        }
      } else if (msg instanceof StopCmdlet) {
        StopCmdlet stopCmdlet = (StopCmdlet) msg;
        cmdletExecutor.stop(stopCmdlet.getCmdletId());
      }
    }
  }

  private class StatusReporter implements Runnable {
    @Override
    public void run() {
      ActionStatusReport report = cmdletExecutor.getActionStatusReport();
      if (!report.getActionStatuses().isEmpty()) {
        report(report);
      }
    }
  }
}
