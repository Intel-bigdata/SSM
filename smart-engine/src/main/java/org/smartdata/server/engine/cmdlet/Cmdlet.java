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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.SmartAction;
import org.smartdata.model.CmdletState;
import org.smartdata.protocol.message.ActionStatus;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

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
  private long stateUpdateTime;
  private final List<SmartAction> actions;
  private List<SmartAction> actionReportList;

  public Cmdlet(List<SmartAction> actions) {
    this.actions = actions;
    this.actionReportList = new ArrayList<>();
    ListIterator<SmartAction> iter = actions.listIterator(actions.size());
    while (iter.hasPrevious()) {
      this.actionReportList.add(iter.previous());
    }
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

  public String toString() {
    return "Rule-" + ruleId + "-Cmd-" + id;
  }

  public boolean isFinished() {
    return CmdletState.isTerminalState(state);
  }

  private void runAllActions() {
    state = CmdletState.EXECUTING;
    stateUpdateTime = System.currentTimeMillis();
    Iterator<SmartAction> iter = actions.iterator();
    while (iter.hasNext()) {
      SmartAction act = iter.next();
      if (act == null) {
        continue;
      }
      // Init Action
      act.init(act.getArguments());
      act.run();
      if (!act.isSuccessful()) {
          while (iter.hasNext()) {
            SmartAction nextAct = iter.next();
            synchronized (this) {
              actionReportList.remove(nextAct);
            }
          }
        state = CmdletState.FAILED;
        stateUpdateTime = System.currentTimeMillis();
        LOG.error("Executing Cmdlet [id={}] meets failed.", getId());
        return;
      }
    }
    state = CmdletState.DONE;
    stateUpdateTime = System.currentTimeMillis();
    // TODO catch MetaStoreException and handle
  }

  @Override
  public void run() {
    runAllActions();
  }

  public synchronized List<ActionStatus> getActionStatuses() throws UnsupportedEncodingException {
    if (actionReportList.isEmpty()) {
      return null;
    }

    // get status in the order of the descend action id.
    // The cmdletmanager should update action status in the ascend order.
    List<ActionStatus> statuses = new ArrayList<>();
    Iterator<SmartAction> iter = actionReportList.iterator();
    while (iter.hasNext()) {
      SmartAction action = iter.next();
      ActionStatus status = action.getActionStatus();
      statuses.add(status);
      if (status.isFinished()) {
        iter.remove();
      }
    }

    return Lists.reverse(statuses);
  }
}
