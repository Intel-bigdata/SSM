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
package org.smartdata.server.cmdlet.hazelcast;

import com.hazelcast.core.ITopic;
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.cmdlet.CmdletFactory;
import org.smartdata.server.cmdlet.executor.CmdletExecutor;
import org.smartdata.server.cmdlet.executor.CmdletStatusReporter;
import org.smartdata.server.cmdlet.message.ActionStatusReport;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.cmdlet.message.StatusMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HazelcastWorker implements CmdletStatusReporter {
  private ScheduledExecutorService executorService;
  private BlockingQueue<LaunchCmdlet> cmdletQueue;
  private CmdletExecutor cmdletExecutor;
  private CmdletFactory factory;
  private Future<?> fetcher;
  private ITopic<StatusMessage> statusTopic;

  public HazelcastWorker(CmdletFactory factory) {
    this.factory = factory;
    this.cmdletExecutor = new CmdletExecutor(this);
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.cmdletQueue =
        HazelcastInstanceProvider.getInstance().getQueue(HazelcastExecutorService.COMMAND_QUEUE);
    this.statusTopic =
        HazelcastInstanceProvider.getInstance().getTopic(HazelcastExecutorService.SLAVE_TO_MASTER);
  }

  public void start() {
    this.fetcher =
        this.executorService.scheduleAtFixedRate(
            new CmdletFetcher(), 1000, 1000, TimeUnit.MILLISECONDS);
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

  private class CmdletFetcher implements Runnable {
    @Override
    public void run() {
      executeCmdlet();
      updateStatus();
    }

    private void executeCmdlet() {
      LaunchCmdlet launchCmdlet = cmdletQueue.poll();
      while (launchCmdlet != null) {
        cmdletExecutor.execute(factory.createCmdlet(launchCmdlet));
        launchCmdlet = cmdletQueue.poll();
      }
    }

    private void updateStatus() {
      ActionStatusReport report = cmdletExecutor.getActionStatusReport();
      if (!report.getActionStatuses().isEmpty()) {
        report(report);
      }
    }
  }
}
