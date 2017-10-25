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
import org.smartdata.action.SmartAction;
import org.smartdata.model.CmdletState;
import org.smartdata.protocol.message.CmdletStatusUpdate;
import org.smartdata.protocol.message.StatusReporter;

/**
 * Action is the minimum unit of execution. A cmdlet can contain more than one
 * actions. Different cmdlets can be executed at the same time, but actions
 * belonging to a cmdlet can only be executed in sequence.
 *
 * <p>The cmdlet get executed when rule conditions fulfills.
 */
// Todo: Cmdlet's state should be maintained by itself
public class Cmdlet implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(Cmdlet.class);
  private long ruleId;   // id of the rule that this cmdlet comes from
  private long id;
  private CmdletState state = CmdletState.NOTINITED;
  private final SmartAction[] actions;
  private final StatusReporter statusReporter;

  public Cmdlet(SmartAction[] actions, StatusReporter reporter) {
    this.statusReporter = reporter;
    this.actions = actions;
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

  public CmdletState getState() {
    return state;
  }

  //Todo: remove this method
  public void setState(CmdletState state) {
    this.state = state;
  }

  public SmartAction[] getActions() {
    return actions == null ? null : actions.clone();
  }

  public String toString() {
    return "Rule-" + ruleId + "-Cmd-" + id;
  }

  public boolean isFinished() {
    return CmdletState.isTerminalState(state);
  }

  private void runAllActions() {
    state = CmdletState.EXECUTING;
    reportCurrentStatus();
    for (SmartAction act : actions) {
      if (act == null) {
        continue;
      }
      // Init Action
      act.init(act.getArguments());
      act.run();
      if (!act.isSuccessful()) {
        state = CmdletState.FAILED;
        reportCurrentStatus();
        return;
      }
    }
    state = CmdletState.DONE;
    // TODO catch MetaStoreException and handle
    reportCurrentStatus();
  }

  @Override
  public void run() {
    runAllActions();
  }

  private void reportCurrentStatus() {
    if (statusReporter != null) {
      statusReporter.report(new CmdletStatusUpdate(id, System.currentTimeMillis(), state));
    }
  }
}
