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

import org.smartdata.actions.hdfs.MoveFileAction;
import org.smartdata.common.CommandState;
import org.smartdata.actions.SmartAction;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.server.metastore.DBAdapter;
import sun.rmi.runtime.Log;

/**
 * Action is the minimum unit of execution. A command can contain more than one
 * actions. Different commands can be executed at the same time, but actions
 * belonging to a command can only be executed in sequence.
 *
 * The command get executed when rule conditions fulfills.
 */
public class Command implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(Command.class);

  private long ruleId;   // id of the rule that this command comes from
  private long id;
  private CommandState state = CommandState.NOTINITED;
  private SmartAction[] actions;
  private String parameters;
  private int currentActionIndex;
  private CommandExecutor.Callback cb;
  private boolean running;

  private long createTime;
  private long scheduleToExecuteTime;
  private long ExecutionCompleteTime;
  private DBAdapter adapter;

  private Command() {

  }

  public Command(SmartAction[] actions, CommandExecutor.Callback cb) {
    this(actions, cb, null);
  }

  public Command(SmartAction[] actions, CommandExecutor.Callback cb,
      DBAdapter adapter) {
    this.actions = actions.clone();
    this.currentActionIndex = 0;
    this.cb = cb;
    this.running = true;
    this.adapter = adapter;
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

  public SmartAction[] getActions() {
    return actions == null ? null : actions.clone();
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
    return (currentActionIndex == actions.length || !running);
  }


  public void runActions() {
    for (SmartAction act : actions) {
      currentActionIndex++;
      if (act == null || !running) {
        continue;
      }
      try {
        // Init Action
        act.init(act.getArguments());
        act.run();
        if (act instanceof MoveFileAction
            && act.getActionStatus().isSuccessful() && adapter != null) {
          String[] args = act.getArguments();
          if (args.length >= 2) {
            adapter.updateFileStoragePolicy(args[0], args[1]);
          }
        }
      } catch (Exception e) {
        LOG.error("Action {} running error! {}", act.getActionStatus().getId(), e);
        act.getActionStatus().end();
        this.setState(CommandState.FAILED);
        break;
      }
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
      this.setState(CommandState.DONE);
    }
  }

  @Override
  public void run() {
    runActions();
    running = false;
    if (cb != null) {
      cb.complete(this.id, this.ruleId, state);
    }
  }
}
