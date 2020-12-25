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
package org.smartdata.protocol.message;

import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;

public class ActionStatusFactory {

  public static final String TIMEOUT_LOG =
      "Timeout error occurred for getting this action's status report.";
  public static final String ACTION_SKIP_LOG = "The action is not executed "
      + "because the prior action in the same cmdlet failed.";
  public static final String SUCCESS_BY_SPECULATION_LOG = ""
      + "The action was successfully executed according to speculation.";

  public static ActionStatus createSkipActionStatus(long cid,
      boolean isLastAction, long aid, long startTime, long finishTime) {
    return new ActionStatus(cid, isLastAction, aid, ACTION_SKIP_LOG,
        startTime, finishTime, new Throwable(), true);
  }

  /**
   * Create timeout action status.
   */
  public static ActionStatus createTimeoutActionStatus(
      CmdletInfo cmdletInfo, ActionInfo actionInfo) {
    long cid = cmdletInfo.getCid();
    long aid = actionInfo.getActionId();
    long startTime = actionInfo.getCreateTime();
    if (startTime == 0) {
      startTime = cmdletInfo.getGenerateTime();
    }
    long finishTime = System.currentTimeMillis();
    long lastAid = cmdletInfo.getAids().get(cmdletInfo.getAids().size() - 1);
    return new ActionStatus(cid, aid == lastAid, aid, TIMEOUT_LOG,
        startTime, finishTime, new Throwable(), true);
  }

  /**
   * Create successful action status by speculation.
   */
  public static ActionStatus createSuccessActionStatus(
      CmdletInfo cmdletInfo, ActionInfo actionInfo) {
    long cid = cmdletInfo.getCid();
    long aid = actionInfo.getActionId();
    long startTime = actionInfo.getCreateTime();
    if (startTime == 0) {
      startTime = cmdletInfo.getGenerateTime();
    }
    long finishTime = System.currentTimeMillis();
    long lastAid = cmdletInfo.getAids().get(cmdletInfo.getAids().size() - 1);
    return new ActionStatus(cid, aid == lastAid, aid,
        SUCCESS_BY_SPECULATION_LOG, startTime, finishTime, null, true);
  }
}
