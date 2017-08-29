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
package org.smartdata.model.action;

import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;

import java.util.List;

public interface ActionScheduler {

  List<String> getSupportedActions();

  /**
   * Called when new action submitted to CmdletManager.
   *
   * @param actionInfo
   * @return acceptable if true, or discard
   */
  boolean onSubmit(ActionInfo actionInfo);

  /**
   * Trying to schedule an action for Dispatch.
   * @param actionInfo
   * @param action
   * @return
   */
  ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action);

  /**
   * Called after and an Cmdlet get scheduled.
   *
   * @param actionInfo
   * @param result
   */
  void postSchedule(ActionInfo actionInfo, ScheduleResult result);

  /**
   *  Called just before dispatch for execution.
   *
   * @param action
   */
  void onPreDispatch(LaunchAction action);

  /**
   *  Called when action finished execution.
   *
   * @param actionInfo
   */
  void onActionFinished(ActionInfo actionInfo);
}
