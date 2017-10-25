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
import org.smartdata.model.CmdletState;
import org.smartdata.protocol.message.ActionFinished;
import org.smartdata.protocol.message.ActionStarted;
import org.smartdata.protocol.message.CmdletStatusUpdate;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
    CmdletExecutor executor = new CmdletExecutor(new SmartConf(), reporter);
    SmartAction action = new HelloAction();
    Map<String, String> args = new HashMap<>();
    args.put(HelloAction.PRINT_MESSAGE, "message");
    action.setStatusReporter(reporter);
    action.setArguments(args);
    action.setActionId(101);
    Cmdlet cmdlet = new Cmdlet(new SmartAction[] {action}, reporter);

    executor.execute(cmdlet);

    Thread.sleep(2000);
    Assert.assertTrue(statusMessages.size() == 4);
    Assert.assertTrue(statusMessages.get(0) instanceof CmdletStatusUpdate);
    Assert.assertTrue(statusMessages.get(1) instanceof ActionStarted);
    Assert.assertTrue(statusMessages.get(2) instanceof ActionFinished);
    Assert.assertNull(((ActionFinished) statusMessages.get(2)).getThrowable());
    Assert.assertTrue(((ActionFinished) statusMessages.get(2)).getResult().contains("message"));
    Assert.assertTrue(statusMessages.get(3) instanceof CmdletStatusUpdate);
    Assert.assertEquals(
        CmdletState.DONE, ((CmdletStatusUpdate) statusMessages.get(3)).getCurrentState());
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
    CmdletExecutor executor = new CmdletExecutor(new SmartConf(), reporter);
    SmartAction action = new HangingAction();
    action.setStatusReporter(reporter);
    action.setActionId(101);
    Cmdlet cmdlet = new Cmdlet(new SmartAction[] {action}, reporter);
    cmdlet.setId(10);

    executor.execute(cmdlet);
    Thread.sleep(1000);
    executor.stop(10L);
    Thread.sleep(2000);

    Assert.assertTrue(statusMessages.size() == 4);
    Assert.assertTrue(statusMessages.get(0) instanceof CmdletStatusUpdate);
    Assert.assertTrue(statusMessages.get(1) instanceof ActionStarted);
    Assert.assertTrue(statusMessages.get(2) instanceof ActionFinished);
    Assert.assertNotNull(((ActionFinished) statusMessages.get(2)).getThrowable());
    Assert.assertTrue(statusMessages.get(3) instanceof CmdletStatusUpdate);
    Assert.assertEquals(
        CmdletState.FAILED, ((CmdletStatusUpdate) statusMessages.get(3)).getCurrentState());
    executor.shutdown();
  }
}
