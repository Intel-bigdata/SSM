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
package org.smartdata.common.message;

import java.io.Serializable;
import java.util.List;

public class ActionStatusReport implements StatusMessage {
  private List<ActionStatus> actionStatuses;

  public ActionStatusReport(List<ActionStatus> actionStatuses) {
    this.actionStatuses = actionStatuses;
  }

  public List<ActionStatus> getActionStatuses() {
    return actionStatuses;
  }

  public void setActionStatuses(List<ActionStatus> actionStatuses) {
    this.actionStatuses = actionStatuses;
  }

  //Todo: integrate this with the other class
  public static class ActionStatus implements Serializable {
    private long actionId;
    private float percentage;
    private String result;
    private String log;

    public ActionStatus(long actionId, float percentage, String result, String log) {
      this.actionId = actionId;
      this.percentage = percentage;
      this.result = result;
      this.log = log;
    }

    public long getActionId() {
      return actionId;
    }

    public void setActionId(long actionId) {
      this.actionId = actionId;
    }

    public float getPencentage() {
      return percentage;
    }

    public void setPencentage(float percentage) {
      this.percentage = percentage;
    }

    public String getResult() {
      return result;
    }

    public void setResult(String result) {
      this.result = result;
    }

    public String getLog() {
      return log;
    }

    public void setLog(String log) {
      this.log = log;
    }
  }
}
