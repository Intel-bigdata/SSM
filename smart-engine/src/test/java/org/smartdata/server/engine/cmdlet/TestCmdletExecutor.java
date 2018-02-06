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

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.HelloAction;
import org.smartdata.action.SmartAction;
import org.smartdata.conf.SmartConf;
import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReport;
import org.smartdata.protocol.message.StatusReporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestCmdletExecutor {

  @Test
  public void testCmdletExecutor() throws InterruptedException {
    final List<StatusMessage> statusMessages = new ArrayList<>();
    StatusReporter reporter =
        new StatusReporter() {
          @Override
          public void report(StatusMessage status) {
            statusMessages.add(status);
          }
        };
    CmdletExecutor executor = new CmdletExecutor(new SmartConf());
    StatusReportTask statusReportTask = new StatusReportTask(reporter, executor);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(
            statusReportTask, 1000, 1000, TimeUnit.MILLISECONDS);
    SmartAction action = new HelloAction();
    Map<String, String> args = new HashMap<>();
    args.put(HelloAction.PRINT_MESSAGE, "message");
    action.setArguments(args);
    action.setActionId(101);
    Cmdlet cmdlet = new Cmdlet(new SmartAction[] {action});

    executor.execute(cmdlet);

    Thread.sleep(2000);

    StatusReport lastReport = (StatusReport) statusMessages.get(statusMessages.size() - 1);
    ActionStatus status = lastReport.getActionStatuses().get(0);
    Assert.assertTrue(status.isFinished());
    Assert.assertNull(status.getThrowable());

    executor.shutdown();
  }

  class HangingAction extends SmartAction {
    @Override
    protected void execute() throws Exception {
      Thread.sleep(10000);
    }
  }

  @Test
  public void testStop() throws InterruptedException {
    final List<StatusMessage> statusMessages = new Vector<>();
    StatusReporter reporter =
        new StatusReporter() {
          @Override
          public void report(StatusMessage status) {
            statusMessages.add(status);
          }
        };
    CmdletExecutor executor = new CmdletExecutor(new SmartConf());
    StatusReportTask statusReportTask = new StatusReportTask(reporter, executor);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(
            statusReportTask, 1000, 1000, TimeUnit.MILLISECONDS);
    SmartAction action = new HangingAction();
    action.setActionId(101);
    Cmdlet cmdlet = new Cmdlet(new SmartAction[] {action});
    cmdlet.setId(10);

    executor.execute(cmdlet);
    Thread.sleep(1000);
    executor.stop(10L);
    Thread.sleep(2000);

    StatusReport lastReport = (StatusReport) statusMessages.get(statusMessages.size() - 1);
    ActionStatus status = lastReport.getActionStatuses().get(0);
    Assert.assertTrue(status.isFinished());
    Assert.assertNotNull(status.getThrowable());
    executor.shutdown();
  }
}
