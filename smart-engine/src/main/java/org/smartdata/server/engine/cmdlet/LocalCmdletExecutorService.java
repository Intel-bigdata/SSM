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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.conf.SmartConf;
import org.smartdata.model.ExecutorType;
import org.smartdata.protocol.message.ActionStatusReport;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReporter;
import org.smartdata.server.cluster.NodeInfo;
import org.smartdata.server.engine.ActiveServerInfo;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.EngineEventBus;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.message.AddNodeMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LocalCmdletExecutorService extends CmdletExecutorService implements StatusReporter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCmdletExecutorService.class);
  private CmdletFactory cmdletFactory;
  private CmdletExecutor cmdletExecutor;
  private ScheduledExecutorService executorService;

  public LocalCmdletExecutorService(SmartConf smartConf, CmdletManager cmdletManager) {
    super(cmdletManager, ExecutorType.LOCAL);
    this.cmdletFactory = new CmdletFactory(cmdletManager.getContext(), this);
    this.cmdletExecutor = new CmdletExecutor(smartConf, this);
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.executorService.scheduleAtFixedRate(
        new StatusFetchTask(), 1000, 1000, TimeUnit.MILLISECONDS);
    EngineEventBus.post(new AddNodeMessage(ActiveServerInfo.getInstance()));
  }

  @Override
  public boolean canAcceptMore() {
    return true;
  }

  public int getNumNodes() {
    return 1;
  }

  public List<NodeInfo> getNodesInfo() {
    // TODO: to be refined
    List<NodeInfo> ret = new ArrayList<>(1);
    ret.add(ActiveServerInfo.getInstance());
    return ret;
  }

  @Override
  public String execute(LaunchCmdlet cmdlet) {
    try {
      this.cmdletExecutor.execute(cmdletFactory.createCmdlet(cmdlet));
      return ActiveServerInfo.getInstance().getId();
    } catch (ActionException e) {
      LOG.error("Failed to execute cmdlet {}" , cmdlet.getCmdletId(), e);
      return null;
    }
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
    LOG.debug("Reporting status message " + status);
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
