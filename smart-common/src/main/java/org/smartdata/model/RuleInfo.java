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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 * Contains info about a rule inside SSM.
 */
public class RuleInfo implements Cloneable {
  private long id;
  private long submitTime;
  private String ruleText;
  private RuleState state;

  // Some static information about rule
  private long numChecked;
  private long numCmdsGen;
  private long lastCheckTime;


  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  public String getRuleText() {
    return ruleText;
  }

  public void setRuleText(String ruleText) {
    this.ruleText = ruleText;
  }

  public RuleState getState() {
    return state;
  }

  public void setState(RuleState state) {
    this.state = state;
  }

  public long getNumChecked() {
    return numChecked;
  }

  public void setNumChecked(long numChecked) {
    this.numChecked = numChecked;
  }

  public long getNumCmdsGen() {
    return numCmdsGen;
  }

  public void setNumCmdsGen(long numCmdsGen) {
    this.numCmdsGen = numCmdsGen;
  }

  public long getLastCheckTime() {
    return lastCheckTime;
  }

  public void setLastCheckTime(long lastCheckTime) {
    this.lastCheckTime = lastCheckTime;
  }

  public void updateRuleInfo(RuleState rs, long lastCheckTime,
      long checkedCount, int cmdletsGen) {
    if (rs != null) {
      this.state = rs;
    }
    if (lastCheckTime != 0) {
      this.lastCheckTime = lastCheckTime;
    }
    this.numChecked += checkedCount;
    this.numCmdsGen += cmdletsGen;
  }

  public RuleInfo() {

  }

  public RuleInfo(long id, long submitTime, String ruleText, RuleState state,
      long numChecked, long numCmdsGen, long lastCheckTime) {
    this.id = id;
    this.submitTime = submitTime;
    this.ruleText = ruleText;
    this.state = state;
    this.numChecked = numChecked;
    this.numCmdsGen = numCmdsGen;
    this.lastCheckTime = lastCheckTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RuleInfo ruleInfo = (RuleInfo) o;
    return id == ruleInfo.id
        && submitTime == ruleInfo.submitTime
        && numChecked == ruleInfo.numChecked
        && numCmdsGen == ruleInfo.numCmdsGen
        && lastCheckTime == ruleInfo.lastCheckTime
        && Objects.equals(ruleText, ruleInfo.ruleText)
        && state == ruleInfo.state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, submitTime, ruleText, state, numChecked, numCmdsGen, lastCheckTime);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    Date submitDate = new Date(submitTime);
    String lastCheck = "Not Checked";
    if (lastCheckTime != 0) {
      Date lastCheckDate = new Date(lastCheckTime);
      lastCheck = sdf.format(lastCheckDate);
    }
    sb.append("{ id = ")
        .append(id)
        .append(", submitTime = '")
        .append(sdf.format(submitDate))
        .append("'")
        .append(", State = ")
        .append(state.toString())
        .append(", lastCheckTime = '")
        .append(lastCheck)
        .append("'")
        .append(", numChecked = ")
        .append(numChecked)
        .append(", numCmdsGen = ")
        .append(numCmdsGen)
        .append(" }");
    return sb.toString();
  }

  public RuleInfo newCopy() {
    return new RuleInfo(id, submitTime, ruleText, state, numChecked,
        numCmdsGen, lastCheckTime);
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static class Builder {
    private long id;
    private long submitTime;
    private String ruleText;
    private RuleState state;
    private long numChecked;
    private long numCmdsGen;
    private long lastCheckTime;

    public static Builder create() {
      return new Builder();
    }

    public Builder setId(long id) {
      this.id = id;
      return this;
    }

    public Builder setSubmitTime(long submitTime) {
      this.submitTime = submitTime;
      return this;
    }

    public Builder setRuleText(String ruleText) {
      this.ruleText = ruleText;
      return this;
    }

    public Builder setState(RuleState state) {
      this.state = state;
      return this;
    }

    public Builder setNumChecked(long numChecked) {
      this.numChecked = numChecked;
      return this;
    }

    public Builder setNumCmdsGen(long numCmdsGen) {
      this.numCmdsGen = numCmdsGen;
      return this;
    }

    public Builder setLastCheckTime(long lastCheckTime) {
      this.lastCheckTime = lastCheckTime;
      return this;
    }

    public RuleInfo build() {
      return new RuleInfo(id, submitTime, ruleText, state, numChecked,
          numCmdsGen, lastCheckTime);
    }
  }
}
