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
package org.smartdata.server.engine.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.smartdata.server.engine.cmdlet.CmdletFactory;
import org.smartdata.server.engine.cmdlet.CmdletExecutor;
import org.smartdata.server.engine.cmdlet.CmdletStatusReporter;
import org.smartdata.server.engine.cmdlet.message.ActionStatusReport;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.cmdlet.message.StatusMessage;
import org.smartdata.server.engine.cmdlet.message.StopCmdlet;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HazelcastWorker implements CmdletStatusReporter {
  private final HazelcastInstance instance;
  private ScheduledExecutorService executorService;
  private ITopic<Serializable> masterMessages;
  private ITopic<StatusMessage> statusTopic;
  private CmdletExecutor cmdletExecutor;
  private CmdletFactory factory;
  private Future<?> fetcher;

  public HazelcastWorker(CmdletFactory factory) {
    this.factory = factory;
    this.cmdletExecutor = new CmdletExecutor(this);
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.instance = HazelcastInstanceProvider.getInstance();
    this.statusTopic = instance.getTopic(HazelcastExecutorService.STATUS_TOPIC);
    String instanceId = instance.getCluster().getLocalMember().getUuid();
    this.masterMessages = instance.getTopic(HazelcastExecutorService.WORKER_TOPIC_PREFIX + instanceId);
    this.masterMessages.addMessageListener(new MasterMessageListener());
  }

  public void start() {
    this.fetcher =
        this.executorService.scheduleAtFixedRate(
            new StatusReporter(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    if (this.fetcher != null) {
      this.fetcher.cancel(true);
    }
    this.executorService.shutdown();
    this.cmdletExecutor.shutdown();
  }

  @Override
  public void report(StatusMessage status) {
    this.statusTopic.publish(status);
  }

  private class MasterMessageListener implements MessageListener<Serializable> {
    @Override
    public void onMessage(Message<Serializable> message) {
      Serializable msg = message.getMessageObject();
      if (msg instanceof LaunchCmdlet) {
        LaunchCmdlet launchCmdlet = (LaunchCmdlet) msg;
        cmdletExecutor.execute(factory.createCmdlet(launchCmdlet));
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
