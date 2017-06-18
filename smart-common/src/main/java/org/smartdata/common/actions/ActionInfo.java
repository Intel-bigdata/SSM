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
package org.smartdata.common.actions;


public class ActionInfo {
  private long actionId;
  private long commandId;
  private String actionName;
  private String[] args;
  private String result;
  private String log;

  private boolean successful;
  private long createTime;
  private boolean finished;
  private long finishTime;

  private float progress;

  public ActionInfo() {

  }

  public ActionInfo(long actionId, long commandId, String actionName,
                    String[] args, String result, String log,
                    boolean successful, long createTime, boolean finished,
                    long finishTime, float progress) {
    this.actionId = actionId;
    this.commandId = commandId;
    this.actionName = actionName;
    this.args = args;
    this.result = result;
    this.log = log;
    this.successful = successful;
    this.createTime = createTime;
    this.finished = finished;
    this.finishTime = finishTime;
    this.progress = progress;
  }

  public long getActionId() {
    return actionId;
  }

  public void setActionId(long actionId) {
    this.actionId = actionId;
  }

  public long getCommandId() {
    return commandId;
  }

  public void setCommandId(long commandId) {
    this.commandId = commandId;
  }

  public String getActionName() {
    return actionName;
  }

  public void setActionName(String actionName) {
    this.actionName = actionName;
  }

  public String[] getArgs() {
    return args;
  }

  public void setArgs(String[] args) {
    this.args = args;
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

  public boolean isSuccessful() {
    return successful;
  }

  public void setSuccessful(boolean successful) {
    this.successful = successful;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public boolean isFinished() {
    return finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static class Builder {
    private long actionId;
    private long commandId;
    private String actionName;
    private String[] args;
    private String result;
    private String log;

    private boolean successful;

    private long createTime;
    private boolean finished;
    private long finishTime;

    private float progress;

    public Builder setActionId(long actionId) {
      this.actionId = actionId;
      return this;
    }

    public Builder setCommandId(long commandId) {
      this.commandId = commandId;
      return this;
    }

    public Builder setActionName(String actionName) {
      this.actionName = actionName;
      return this;
    }

    public Builder setArgs(String[] args) {
      this.args = args;
      return this;
    }

    public Builder setResult(String result) {
      this.result = result;
      return this;
    }

    public Builder setLog(String log) {
      this.log = log;
      return this;
    }

    public Builder setSuccessful(boolean successful) {
      this.successful = successful;
      return this;
    }

    public Builder setCreateTime(long createTime) {
      this.createTime = createTime;
      return this;
    }

    public Builder setFinished(boolean finished) {
      this.finished = finished;
      return this;
    }

    public Builder setFinishTime(long finishTime) {
      this.finishTime = finishTime;
      return this;
    }

    public Builder setProgress(float progress) {
      this.progress = progress;
      return this;
    }

    public static Builder create() {
      return new Builder();
    }

    public ActionInfo build() {
      return new ActionInfo(actionId,commandId,actionName, args, result,
          log, successful, createTime, finished, finishTime, progress);
    }
  }
}