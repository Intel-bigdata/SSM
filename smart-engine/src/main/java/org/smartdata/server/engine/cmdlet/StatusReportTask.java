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

import java.util.List;

public class StatusReportTask implements Runnable {
  private StatusReporter statusReporter;
  private CmdletExecutor cmdletExecutor;
  public static int REPORT_THRESHOLD = 50;
  public static double FINISHED_RATIO = 0.2;

  public StatusReportTask(StatusReporter statusReporter, CmdletExecutor cmdletExecutor) {
    this.statusReporter = statusReporter;
    this.cmdletExecutor = cmdletExecutor;
  }

  @Override
  public void run() {
    StatusReport statusReport = cmdletExecutor.getStatusReport();
    if (statusReport != null) {
      List<ActionStatus> actionStatuses = statusReport.getActionStatuses();
      if (!actionStatuses.isEmpty()) {
        int finishedNum = 0;
        for (ActionStatus actionStatus : actionStatuses) {
          if (actionStatus.isFinished()) {
            finishedNum++;
          }
        }
        if (actionStatuses.size() < REPORT_THRESHOLD ||
            finishedNum / actionStatuses.size() > FINISHED_RATIO) {
          statusReporter.report(statusReport);
        }
      }
    }
  }
}
