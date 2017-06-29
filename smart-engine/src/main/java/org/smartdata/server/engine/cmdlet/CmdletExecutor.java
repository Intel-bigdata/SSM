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

import org.smartdata.actions.SmartAction;
import org.smartdata.common.message.ActionStatus;
import org.smartdata.common.message.StatusReporter;
import org.smartdata.common.message.ActionStatusReport;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

//Todo: 1. make this a interface so that we could have different executor implementation
//      2. add api providing available resource
public class CmdletExecutor {
  private final StatusReporter reporter;
  private final SmartConf smartConf;
  private Map<Long, Future> listenableFutures;
  private Map<Long, Cmdlet> runningCmdlets;
  private ExecutorService executorService;

  public CmdletExecutor(SmartConf smartConf, StatusReporter reporter) {
    this.reporter = reporter;
    this.smartConf = smartConf;
    this.listenableFutures = new ConcurrentHashMap<>();
    this.runningCmdlets = new ConcurrentHashMap<>();
    int nThreads =
        smartConf.getInt(
            SmartConfKeys.SMART_CMDLET_EXECUTOR_THREAD_SIZE,
            SmartConfKeys.SMART_CMDLET_EXECUTOR_THREAD_SIZE_DEFAULT);
    this.executorService = Executors.newFixedThreadPool(nThreads);
  }

  public void execute(Cmdlet cmdlet) {
    Future<?> future = this.executorService.submit(cmdlet);
    this.listenableFutures.put(cmdlet.getId(), future);
    this.runningCmdlets.put(cmdlet.getId(), cmdlet);
  }

  public void stop(Long cmdletId) {
    if (this.listenableFutures.containsKey(cmdletId)) {
      this.listenableFutures.get(cmdletId).cancel(true);
    }
    removeCmdlet(cmdletId);
  }

  public void shutdown() {
    this.executorService.shutdown();
  }

  public ActionStatusReport getActionStatusReport() {
    List<ActionStatus> actionStatusList = new ArrayList<>();
    for (Cmdlet cmdlet : this.runningCmdlets.values()) {
      for (SmartAction action : cmdlet.getActions()) {
        try {
          actionStatusList.add(action.getActionStatus());
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
      }
    }
    return new ActionStatusReport(actionStatusList);
  }

  private void removeCmdlet(long cmdletId) {
    this.runningCmdlets.remove(cmdletId);
    this.listenableFutures.remove(cmdletId);
  }
}
