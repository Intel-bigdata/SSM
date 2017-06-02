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
package org.smartdata.server.command;

import org.smartdata.common.CommandState;
import org.smartdata.actions.SmartAction;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Command is the minimum unit of execution. Different commands can be
 * executed at the same time, but smartActions belongs to an SmartAction can only be
 * executed in serial.
 *
 * The command get executed when rule conditions fulfilled.
 * A command can have many smartActions, for example:
 * Command 'archive' contains the following two smartActions:
 *  1.) SetStoragePolicy
 *  2.) EnforceStoragePolicy
 */
public class Command implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(Command.class);

  private long ruleId;   // id of the rule that this Command comes from
  private long id;
  private CommandState state = CommandState.NOTINITED;
  private SmartAction[] smartActions;
  private String parameters;
  private int currentActionIndex;
  CommandExecutor.Callback cb;
  private boolean running;

  private long createTime;
  private long scheduleToExecuteTime;
  private long ExecutionCompleteTime;

  private Command() {

  }

  public Command(SmartAction[] smartActions, CommandExecutor.Callback cb) {
    this.smartActions = smartActions.clone();
    this.currentActionIndex = 0;
    this.cb = cb;
    this.running = true;
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public long getId() {
    return id;
  }

  public String getParameter() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  public CommandState getState() {
    return state;
  }

  public void setState(CommandState state) {
    this.state = state;
  }

  public SmartAction[] getSmartActions() {
    return smartActions == null ? null : smartActions.clone();
  }

  public long getScheduleToExecuteTime() {
    return scheduleToExecuteTime;
  }

  public void setScheduleToExecuteTime(long time) {
    this.scheduleToExecuteTime = time;
  }

  public String toString() {
    return "Rule-" + ruleId + "-Cmd-" + id;
  }

  public void stop() throws IOException {
    LOG.info("Command {} Stopped!", toString());
    //TODO Force Stop Command
    running = false;
  }

  public boolean isFinished() {
    return (currentActionIndex == smartActions.length || !running);
  }


  public void runSmartActions() {
    for (SmartAction act : smartActions) {
      currentActionIndex++;
      if (act == null || !running) {
        continue;
      }
      act.run();
      // Run actions sequentially!
      while (!act.getActionStatus().isFinished()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          if (!running) {
            break;
          }
        }
      }
    }
  }

  @Override
  public void run() {
    runSmartActions();
    running = false;
    if (cb != null) {
      cb.complete(this.id, this.ruleId, CommandState.DONE);
    }
  }
}
