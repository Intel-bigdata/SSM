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
package org.smartdata.actions;

import org.smartdata.SmartContext;

import java.io.PrintStream;
import java.util.UUID;

/**
 * Smart action, the base class. All actions should inherit this. All actions should be able to run in a command line
 * or web console. User defined actions are also meant to extend this.
 */
public abstract class SmartAction {
  private String[] actionArgs;
  private SmartContext context;
  private ActionStatus actionStatus;
  private PrintStream resultOut;
  private PrintStream logOut;

  public SmartContext getContext() {
    return context;
  }

  public void setContext(SmartContext context) {
    this.context = context;
  }

  public void setActionStatus(ActionStatus actionStatus) {
    this.actionStatus = actionStatus;
    resultOut = actionStatus.getResultPrintStream();
    logOut = actionStatus.getLogPrintStream();
  }

  /**
   * Used to initialize the action.
   * @param args Action specific
   */
  public void init(String[] args) {
    this.actionArgs = args;
  }

  /**
   * Get action arguments.
   * @return
   */
  public String[] getArguments() {
    return actionArgs;
  }

  /**
   * Execute an action.
   * @return a uid to track the status of the action, or null if the action
   * has no status.
   */
  protected abstract UUID execute();

  public UUID run() {
    return execute();
  }
}
