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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class FileDiff {
  private long diffId;
  private long ruleId;
  private FileDiffType diffType;
  private String src;
  private Map<String, String> parameters;
  private FileDiffState state;
  private long createTime;

  public FileDiff() {
    this.createTime = System.currentTimeMillis();
    this.parameters = new HashMap<>();
  }

  public FileDiff(FileDiffType diffType) {
    this();
    this.diffType = diffType;
    this.state = FileDiffState.PENDING;
  }

  public FileDiff(FileDiffType diffType, FileDiffState state) {
    this();
    this.diffType = diffType;
    this.state = state;
  }

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

  public String getParametersString() {
    StringBuffer ret = new StringBuffer();
    if (parameters.containsKey("-dest")) {
      ret.append(String.format(" -dest %s", parameters.get("-dest")));
      parameters.remove("-dest");
    }
    for (Iterator<Map.Entry<String, String>> it = parameters.entrySet().iterator(); it.hasNext();) {
      Map.Entry<String, String> entry = it.next();
        ret.append(String.format(" %s %s", entry.getKey(), entry.getValue()));
    }
    return ret.toString();
  }


  public FileDiffState getState() {
    return state;
  }

  public void setState(FileDiffState state) {
    this.state = state;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileDiff fileDiff = (FileDiff) o;
    return diffId == fileDiff.diffId
        && ruleId == fileDiff.ruleId
        && createTime == fileDiff.createTime
        && diffType == fileDiff.diffType
        && Objects.equals(src, fileDiff.src)
        && Objects.equals(parameters, fileDiff.parameters)
        && state == fileDiff.state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(diffId, ruleId, diffType, src, parameters, state, createTime);
  }

  @Override
  public String toString() {
    return String.format(
        "FileDiff{diffId=%s, parameters=%s, src=%s, diffType=%s, state=%s, createTime=%s}",
        diffId, parameters, src, diffType, state.getValue(), createTime);
  }
}
