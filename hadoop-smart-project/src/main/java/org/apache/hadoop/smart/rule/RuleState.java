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
package org.apache.hadoop.smart.rule;

/**
 * The possible state that a rule can be in.
 */
public enum RuleState {
  ACTIVE(0),      // functioning
  DRYRUN(1),      // without execute the rule commands
  DISABLED(2),    // stop maintain info for the rule
  FINISHED(3),    // for one-shot rule
  DELETED(4);

  private int value;

  private RuleState(int value) {
    this.value = value;
  }

  public static RuleState fromValue(int value) {
    for (RuleState r : values()) {
      if (value == r.getValue()) {
        return r;
      }
    }
    return null;
  }

  public static RuleState fromName(String name) {
    for (RuleState r : values()) {
      if (r.toString().equalsIgnoreCase(name)) {
        return r;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }
}
