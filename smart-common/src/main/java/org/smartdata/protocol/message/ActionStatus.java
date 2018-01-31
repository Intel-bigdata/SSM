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

import java.io.Serializable;

public class ActionStatus implements Serializable {
  private long actionId;
  private float percentage;
  private String result;
  private String log;
  private long startTime;
  private long finishTime;
  private Throwable throwable;
  private boolean finished;

  public ActionStatus(long actionId, float percentage, String result, String log,
                      long startTime, long finishTime, Throwable t, boolean finished) {
    this.actionId = actionId;
    this.percentage = percentage;
    this.result = result;
    this.log = log;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.throwable = t;
    this.finished = finished;
  }

  public ActionStatus(long actionId, String log, long startTime,
                      long finishTime, Throwable t, boolean finished) {
    this.actionId = actionId;
    this.log = log;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.throwable = t;
    this.finished = finished;
  }

  public ActionStatus(long actionId, long finishTime) {
    this.actionId = actionId;
    this.finishTime = finishTime;
  }

  public ActionStatus(long actionId, long startTime, Throwable t) {
    this.actionId = actionId;
    this.startTime = startTime;
    this.throwable = t;
  }

  public long getActionId() {
    return actionId;
  }

  public void setActionId(long actionId) {
    this.actionId = actionId;
  }

  public float getPercentage() {
    return percentage;
  }

  public void setPercentage(float percentage) {
    this.percentage = percentage;
  }

  public String getResult() {
    if (result == null) {
      return "";
    }
    return result;
  }

  public void setResult(String result) {
    this.result = result;
  }

  public String getLog() {
    if (log == null) {
      return "";
    }
    return log;
  }

  public void setLog(String log) {
    this.log = log;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public Throwable getThrowable() {
    return throwable;
  }

  public boolean isFinished() {
    return finished;
  }
}
