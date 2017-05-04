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
package org.apache.hadoop.ssm.sql;

import org.apache.hadoop.ssm.CommandState;
import org.apache.hadoop.ssm.actions.ActionType;

public class CommandInfo {
  private long cid;
  private long rid;
  private ActionType actionId;
  private CommandState state;
  private String parameters;
  private long generateTime;
  private long stateChangedTime;

  public CommandInfo(long cid, long rid, ActionType actionId, CommandState state
      , String parameters, long generateTime, long stateChangedTime) {
    this.cid = cid;
    this.rid = rid;
    this.actionId = actionId;
    this.state = state;
    this.parameters = parameters;
    this.generateTime = generateTime;
    this.stateChangedTime = stateChangedTime;
  }

  public long getCid() {
    return cid;
  }
  public void setCid(int cid) {
    this.cid = cid;
  }
  public long getRid() {
    return rid;
  }
  public void setRid(int rid) {
    this.rid = rid;
  }
  public ActionType getActionId() {
    return actionId;
  }
  public void setActionId(ActionType actionId) {
    this.actionId = actionId;
  }
  public CommandState getState() {
    return state;
  }
  public void setState(CommandState state) {
    this.state = state;
  }
  public String getParameters() {
    return parameters;
  }
  public void setParameters(String parameters) {
    this.parameters = parameters;
  }
  public long getGenerateTime() {
    return generateTime;
  }
  public void setGenerateTime(long generateTime) {
    this.generateTime = generateTime;
  }
  public long getStateChangedTime() {
    return stateChangedTime;
  }
  public void setStateChangedTime(long stateChangedTime) {
    this.stateChangedTime = stateChangedTime;
  }
}
