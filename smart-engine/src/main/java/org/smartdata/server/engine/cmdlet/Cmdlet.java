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

import org.smartdata.common.CmdletState;
import org.smartdata.actions.SmartAction;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action is the minimum unit of execution. A cmdlet can contain more than one
 * actions. Different cmdlets can be executed at the same time, but actions
 * belonging to a cmdlet can only be executed in sequence.
 *
 * The cmdlet get executed when rule conditions fulfills.
 */
public class Cmdlet implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(Cmdlet.class);

  private long ruleId;   // id of the rule that this cmdlet comes from
  private long id;
  private CmdletState state = CmdletState.NOTINITED;
  private SmartAction[] actions;
  private String parameters;
  private int currentActionIndex;
  private volatile boolean running;

  private long scheduleToExecuteTime;

  public Cmdlet(SmartAction[] actions) {
    this.actions = actions.clone();
    this.currentActionIndex = 0;
    this.running = false;
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

  public CmdletState getState() {
    return state;
  }

  public void setState(CmdletState state) {
    this.state = state;
  }

  public SmartAction[] getActions() {
    return actions == null ? null : actions.clone();
  }

  public void setScheduleToExecuteTime(long time) {
    this.scheduleToExecuteTime = time;
  }

  public String toString() {
    return "Rule-" + ruleId + "-Cmd-" + id;
  }

  public void stop() throws IOException {
    LOG.info("Cmdlet {} Stopped!", toString());
    //TODO Force Stop Cmdlet
    running = false;
  }

  public boolean isFinished() {
    return (currentActionIndex == actions.length || !running);
  }

  private void runActions() {
    for (SmartAction act : actions) {
      currentActionIndex++;
      if (act == null || !running) {
        continue;
      }
      // Init Action
      act.init(act.getArguments());
      act.run();
      if (!act.isSuccessful()) {
        this.setState(CmdletState.FAILED);
        break;
      }
    }
    this.setState(CmdletState.DONE);
  }

  @Override
  public void run() {
    running = true;
    runActions();
    running = false;
  }
}
