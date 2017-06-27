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
package org.smartdata.server.engine.cmdlet;

import org.smartdata.common.message.ActionStatusReport;
import org.smartdata.common.message.StatusReporter;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.common.message.StatusMessage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LocalCmdletExecutorService extends CmdletExecutorService implements StatusReporter {
  private CmdletFactory cmdletFactory;
  private CmdletExecutor cmdletExecutor;
  private ScheduledExecutorService executorService;

  public LocalCmdletExecutorService(CmdletManager cmdletManager) {
    super(cmdletManager);
    this.cmdletFactory = new CmdletFactory(cmdletManager.getContext(), this);
    this.cmdletExecutor = new CmdletExecutor(this);
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.executorService.scheduleAtFixedRate(new StatusFetchTask(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isLocalService() {
    return true;
  }

  @Override
  public boolean canAcceptMore() {
    return true;
  }

  @Override
  public void execute(LaunchCmdlet cmdlet) {
    this.cmdletExecutor.execute(cmdletFactory.createCmdlet(cmdlet));
  }

  @Override
  public void stop(long cmdletId) {
    this.cmdletExecutor.stop(cmdletId);
  }

  @Override
  public void shutdown() {
    this.executorService.shutdown();
    this.cmdletExecutor.shutdown();
  }

  @Override
  public void report(StatusMessage status) {
    cmdletManager.updateStatus(status);
  }

  private class StatusFetchTask implements Runnable {
    @Override
    public void run() {
      ActionStatusReport statusReport = cmdletExecutor.getActionStatusReport();
      if (statusReport.getActionStatuses().size() > 0) {
        report(statusReport);
      }
    }
  }
}
