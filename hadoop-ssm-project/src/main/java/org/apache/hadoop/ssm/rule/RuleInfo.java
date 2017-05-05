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
package org.apache.hadoop.ssm.rule;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Contains info about a rule inside SSM.
 */
public class RuleInfo implements Cloneable {
  private long id;
  private long submitTime;
  private String ruleText;
  private RuleState state;

  // Some static information about rule
  private long countConditionChecked;
  private long countConditionFulfilled;
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

  public RuleState getState() {
    return state;
  }

  public void setState(RuleState state) {
    this.state = state;
  }

  public long getCountConditionChecked() {
    return countConditionChecked;
  }

  public void setCountConditionChecked(long numChecked) {
    this.countConditionChecked = numChecked;
  }

  public long increaseCountConditionChecked() {
    this.countConditionChecked++;
    return this.countConditionChecked;
  }

  public long getCountConditionFulfilled() {
    return countConditionFulfilled;
  }

  public void setCountConditionFulfilled(long numConditionFulfilled) {
    this.countConditionFulfilled = numConditionFulfilled;
  }

  public long increaseCountConditionFulfilled(int n) {
    countConditionFulfilled += n;
    return countConditionFulfilled;
  }

  public long getLastCheckTime() {
    return lastCheckTime;
  }

  public void setLastCheckTime(long lastCheckTime) {
    this.lastCheckTime = lastCheckTime;
  }

  public void updateRuleInfo(RuleState rs, long lastCheckTime,
      long checkedCount, int commandsGen) {
    if (rs != null) {
      this.state = rs;
    }
    if (lastCheckTime != 0) {
      this.lastCheckTime = lastCheckTime;
    }
    this.countConditionChecked += checkedCount;
    this.countConditionFulfilled += commandsGen;
  }

  public RuleInfo(long id, long submitTime, String ruleText, RuleState state,
      long countConditionChecked, long countConditionFulfilled,
      long lastCheckTime) {
    this.id = id;
    this.submitTime = submitTime;
    this.ruleText = ruleText;
    this.state = state;
    this.countConditionChecked = countConditionChecked;
    this.countConditionFulfilled = countConditionFulfilled;
    this.lastCheckTime = lastCheckTime;
  }

  public boolean equals(RuleInfo info) {
    return info != null && id == info.id && submitTime == info.submitTime
        && ruleText.equals(info.ruleText) && state == info.state
        && countConditionChecked == info.countConditionChecked
        && countConditionFulfilled == info.countConditionFulfilled
        && lastCheckTime == info.lastCheckTime;
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
    sb.append("{ id = ").append(id)
        .append(", submitTime = '").append(sdf.format(submitDate)).append("'")
        .append(", State = ").append(state.toString())
        .append(", lastCheckTime = '").append(lastCheck).append("'")
        .append(", countConditionChecked = ").append(countConditionChecked)
        .append(", countConditionFulfilled = ")
        .append(countConditionFulfilled)
        .append(" }");
    return sb.toString();
  }

  public RuleInfo newCopy() {
    return new RuleInfo(id, submitTime, ruleText, state, countConditionChecked,
        countConditionFulfilled, lastCheckTime);
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static class Builder {
    private long id;
    private long submitTime;
    private String ruleText;
    private RuleState state;
    private long countConditionChecked;
    private long countConditionFulfilled;
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

    public Builder setCountConditionChecked(long countConditionChecked) {
      this.countConditionChecked = countConditionChecked;
      return this;
    }

    public Builder setCountConditionFulfilled(long countConditionFulfilled) {
      this.countConditionFulfilled = countConditionFulfilled;
      return this;
    }

    public Builder setLastCheckTime(long lastCheckTime) {
      this.lastCheckTime = lastCheckTime;
      return this;
    }

    public RuleInfo build() {
      return new RuleInfo(id, submitTime, ruleText, state, countConditionChecked,
          countConditionFulfilled, lastCheckTime);
    }
  }
}