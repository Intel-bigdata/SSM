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

public class ActionFinished implements StatusMessage {
  private long actionId;
  private long timestamp;
  private String result;
  private String log;
  private Throwable throwable;

  public ActionFinished(long actionId, long timestamp, Throwable t) {
    this(actionId, timestamp, "", "", t);
  }

  public ActionFinished(long actionId, long timestamp, String result, String log) {
    this(actionId, timestamp, result, log, null);
  }

  public ActionFinished(
      long actionId, long timestamp, String result, String log, Throwable throwable) {
    this.actionId = actionId;
    this.timestamp = timestamp;
    this.result = result;
    this.log = log;
    this.throwable = throwable;
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

  public long getActionId() {
    return actionId;
  }

  public void setActionId(long actionId) {
    this.actionId = actionId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Throwable getThrowable() {
    return throwable;
  }

  public void setThrowable(Throwable throwable) {
    this.throwable = throwable;
  }

  @Override
  public String toString() {
    if (throwable == null) {
      return String.format("Action %s finished at %s", actionId, timestamp);
    } else {
      return String.format(
          "Action %s finished at %s with exception %s", actionId, timestamp, throwable);
    }
  }
}
