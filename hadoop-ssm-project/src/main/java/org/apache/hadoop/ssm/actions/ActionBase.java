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
package org.apache.hadoop.ssm.actions;

import org.apache.hadoop.hdfs.DFSClient;

import java.util.UUID;

/**
 * Base for actions
 */
public abstract class ActionBase {
  protected ActionType actionType;
  protected DFSClient dfsClient;

  public ActionBase(DFSClient client) {
    this.dfsClient = client;
  }

  /**
   * Used to initialize the action.
   * @param args Action specific
   */
  public abstract ActionBase initial(String[] args);

  /**
   * Execute an action.
   * @return a uid to track the status of the action, or null if the action
   * has no status.
   */
  protected abstract UUID execute();

  public ActionType getActionType() {
    return actionType;
  }

  public final boolean run() {
    execute();
    return true;
  }
}
