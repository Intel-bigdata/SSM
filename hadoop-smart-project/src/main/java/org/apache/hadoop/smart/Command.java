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
package org.apache.hadoop.smart;

import org.apache.hadoop.smart.actions.ActionBase;
import org.apache.hadoop.smart.actions.ActionExecutor;
import org.apache.hadoop.smart.mover.MoverPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command is the minimum unit of execution. Different commands can be
 * executed at the same time, but actions belongs to an action can only be
 * executed in serial.
 *
 * The command get executed when rule conditions fulfilled.
 * A command can have many actions, for example:
 * Command 'archive' contains the following two actions:
 *  1.) SetStoragePolicy
 *  2.) EnforceStoragePolicy
 */


public class Command implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(Command.class);

  private long ruleId;   // id of the rule that this Command comes from
  private long id;
  private CommandState state = CommandState.NOTINITED;
  private ActionBase[] actions;
  private Map<String, String> parameters;
  CommandExecutor.Callback cb;
  private ArrayList<UUID> uuids;
  private boolean running;

  private long createTime;
  private long scheduleToExecuteTime;
  private long ExecutionCompleteTime;

  private Command() {

  }

  public Command(ActionBase[] actions, CommandExecutor.Callback cb) {
    this.actions = actions.clone();
    this.cb = cb;
    this.uuids = new ArrayList<>();
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

  public Map<String, String> getParameter() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public CommandState getState() {
    return state;
  }

  public void setState(CommandState state) {
    this.state = state;
  }

  public ActionBase[] getActions() {
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
    running = false;
    if(uuids.size() != 0) {
      MoverPool moverPool = MoverPool.getInstance();
      try {
        for (UUID id : uuids) {
          if (!moverPool.getStatus(id).getIsFinished()) {
              moverPool.stop(id);
          }
        }
      } catch (Exception e) {
        LOG.error("Shutdown Unfinished Actions Error!");
        throw new IOException(e);
      }
    }
  }

  public void runActions() {
    UUID uid;
    for (ActionBase act : actions) {
      if (act == null)
        continue;
      uid = ActionExecutor.run(act);
      if (uid == null)
        continue;
      uuids.add(uid);
    }
    MoverPool moverPool = MoverPool.getInstance();
    while (uuids.size() != 0 && running) {
      for (Iterator<UUID> iter = uuids.iterator(); iter.hasNext(); ) {
        UUID id = iter.next();
        if (moverPool.getStatus(id).getIsFinished()) {
          moverPool.removeStatus(id);
          iter.remove();
        }
      }
      if (uuids.size() == 0 || !running)
        break;
      try {
        Thread.sleep(300);
      } catch (Exception e) {
        LOG.error("Run Action Thread error!");
      }
    }
  }

  @Override
  public void run() {
    runActions();
    if (cb != null && running)
      cb.complete(this.id, this.ruleId, CommandState.DONE);
  }
}