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

/**
 * Contains info about a rule inside SSM.
 */
public class RuleInfo {
  private final long id;
  private final long submitTime;
  private final String ruleText;
  private final RuleState state;

  // Some static information about rule
  private final long countConditionChecked;
  private final long countConditionFulfilled;

  public long getId() {
    return id;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public String getRuleText() {
    return ruleText;
  }

  public RuleState getState() {
    return state;
  }

  public long getCountConditionChecked() {
    return countConditionChecked;
  }

  public long getCountConditionFulfilled() {
    return countConditionFulfilled;
  }

  public RuleInfo(long id, long submitTime, String ruleText, RuleState state,
                  long countConditionChecked, long countConditionFulfilled) {
    this.id = id;
    this.submitTime = submitTime;
    this.ruleText = ruleText;
    this.state = state;
    this.countConditionChecked = countConditionChecked;
    this.countConditionFulfilled = countConditionFulfilled;
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

    public RuleInfo build() {
      return new RuleInfo(id, submitTime, ruleText, state, countConditionChecked,
          countConditionFulfilled);
    }
  }
}