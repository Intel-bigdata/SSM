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
package org.smartdata.model;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.Map;

public class ActionInfo {
  private long actionId;
  private long cmdletId;
  private String actionName;
  private Map<String, String> args;
  private String result;
  private String log;

  private boolean successful;
  private long createTime;
  private boolean finished;
  private long finishTime;

  private float progress;

  public ActionInfo() {
  }

  public ActionInfo(long actionId, long cmdletId, String actionName,
      Map<String, String> args, String result, String log,
      boolean successful, long createTime, boolean finished,
      long finishTime, float progress) {
    this.actionId = actionId;
    this.cmdletId = cmdletId;
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

  public long getCmdletId() {
    return cmdletId;
  }

  public void setCmdletId(long cmdletId) {
    this.cmdletId = cmdletId;
  }

  public String getActionName() {
    return actionName;
  }

  public void setActionName(String actionName) {
    this.actionName = actionName;
  }

  public Map<String, String> getArgs() {
    return args;
  }

  public void setArgs(Map<String, String> args) {
    this.args = args;
  }

  public String getArgsJsonString() {
    Gson gson = new Gson();
    return gson.toJson(args);
  }

  public void setArgsFromJsonString(String jsonArgs) {
    Gson gson = new Gson();
    args = gson.fromJson(jsonArgs,
        new TypeToken<Map<String, String>>() {
        }.getType());
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ActionInfo that = (ActionInfo) o;

    if (actionId != that.actionId) return false;
    if (cmdletId != that.cmdletId) return false;
    if (successful != that.successful) return false;
    if (createTime != that.createTime) return false;
    if (finished != that.finished) return false;
    if (finishTime != that.finishTime) return false;
    if (Float.compare(that.progress, progress) != 0) return false;
    if (actionName != null ? !actionName.equals(that.actionName) : that.actionName != null) return false;
    if (args != null ? !args.equals(that.args) : that.args != null) return false;
    if (result != null ? !result.equals(that.result) : that.result != null) return false;
    return log != null ? log.equals(that.log) : that.log == null;
  }

  @Override
  public int hashCode() {
    int result1 = (int) (actionId ^ (actionId >>> 32));
    result1 = 31 * result1 + (int) (cmdletId ^ (cmdletId >>> 32));
    result1 = 31 * result1 + (actionName != null ? actionName.hashCode() : 0);
    result1 = 31 * result1 + (args != null ? args.hashCode() : 0);
    result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
    result1 = 31 * result1 + (log != null ? log.hashCode() : 0);
    result1 = 31 * result1 + (successful ? 1 : 0);
    result1 = 31 * result1 + (int) (createTime ^ (createTime >>> 32));
    result1 = 31 * result1 + (finished ? 1 : 0);
    result1 = 31 * result1 + (int) (finishTime ^ (finishTime >>> 32));
    result1 = 31 * result1 + (progress != +0.0f ? Float.floatToIntBits(progress) : 0);
    return result1;
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static class Builder {
    private long actionId;
    private long cmdletId;
    private String actionName;
    private Map<String, String> args;
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

    public Builder setCmdletId(long cmdletId) {
      this.cmdletId = cmdletId;
      return this;
    }

    public Builder setActionName(String actionName) {
      this.actionName = actionName;
      return this;
    }

    public Builder setArgs(Map<String, String> args) {
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
      return new ActionInfo(actionId, cmdletId, actionName, args, result,
          log, successful, createTime, finished, finishTime, progress);
    }
  }
}