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

public class FileDiff {
  private long diffId;
  private long ruleId;
  private FileDiffType diffType;
  private String src;
  private Map<String, String> parameters;
  private FileDiffState state;
  private long create_time;

  public long getDiffId() {
    return diffId;
  }

  public void setDiffId(long diffId) {
    this.diffId = diffId;
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public FileDiffType getDiffType() {
    return diffType;
  }

  public void setDiffType(FileDiffType diffType) {
    this.diffType = diffType;
  }

  public String getSrc() {
    return src;
  }

  public void setSrc(String src) {
    this.src = src;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String>  parameters) {
    this.parameters = parameters;
  }

  public String getParametersJsonString() {
    Gson gson = new Gson();
    return gson.toJson(parameters);
  }

  public void setParametersFromJsonString(String jsonParameters) {
    Gson gson = new Gson();
    parameters = gson.fromJson(jsonParameters,
        new TypeToken<Map<String, String>>() {
        }.getType());
  }



  public FileDiffState getState() {
    return state;
  }

  public void setState(FileDiffState state) {
    this.state = state;
  }

  public long getCreate_time() {
    return create_time;
  }

  public void setCreate_time(long create_time) {
    this.create_time = create_time;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FileDiff fileDiff = (FileDiff) o;

    if (diffId != fileDiff.diffId) return false;
    if (state != fileDiff.state) return false;
    if (create_time != fileDiff.create_time) return false;
    if (parameters != null ? !parameters.equals(fileDiff.parameters) : fileDiff.parameters != null) return false;
    if (src != null ? !src.equals(fileDiff.src) : fileDiff.src != null) return false;
    return diffType == fileDiff.diffType;
  }

  @Override
  public int hashCode() {
    int result = (int) (getDiffId() ^ (getDiffId() >>> 32));
    result = 31 * result + (int) (getRuleId() ^ (getRuleId() >>> 32));
    result = 31 * result + getDiffType().hashCode();
    result = 31 * result + getSrc().hashCode();
    result = 31 * result + getParameters().hashCode();
    result = 31 * result + getState().hashCode();
    result = 31 * result + (int) (getCreate_time() ^ (getCreate_time() >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return String.format("FileDiff{diffId=%s, parameters=%s, " +
            "src=%s, diffType=%s, state=%s, create_time=%s}", diffId, parameters, src,
        diffType, state.getValue(), create_time);
  }
}
