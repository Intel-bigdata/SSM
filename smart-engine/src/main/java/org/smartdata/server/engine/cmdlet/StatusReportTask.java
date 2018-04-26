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

import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.StatusReport;
import org.smartdata.protocol.message.StatusReporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatusReportTask implements Runnable {
  private StatusReporter statusReporter;
  private CmdletExecutor cmdletExecutor;
  private long lastReportTime;
  private int interval;
  private Map<Long, ActionStatus> idToActionStatus;
  public static final int TIME_MULTIPLIER = 5;
  public static final double FINISHED_RATIO = 0.2;

  public StatusReportTask(
      StatusReporter statusReporter, CmdletExecutor cmdletExecutor, int period) {
    this.statusReporter = statusReporter;
    this.cmdletExecutor = cmdletExecutor;
    this.lastReportTime = System.currentTimeMillis();
    this.interval = TIME_MULTIPLIER * period;
    this.idToActionStatus = new HashMap<>();
  }

  @Override
  public void run() {
    StatusReport statusReport = cmdletExecutor.getStatusReport();
    if (statusReport != null) {
      List<ActionStatus> actionStatuses = statusReport.getActionStatuses();
      for (ActionStatus actionStatus : actionStatuses) {
        idToActionStatus.put(actionStatus.getActionId(), actionStatus);
      }
      if (!idToActionStatus.values().isEmpty()) {
        int finishedNum = 0;
        for (ActionStatus actionStatus : idToActionStatus.values()) {
          if (actionStatus.isFinished()) {
            finishedNum++;
          }
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastReportTime >= interval
            || (float) finishedNum / actionStatuses.size() >= FINISHED_RATIO) {
          statusReporter.report(new StatusReport(new ArrayList(idToActionStatus.values())));
          idToActionStatus.clear();
          lastReportTime = currentTime;
        }
      }
    }
  }
}
