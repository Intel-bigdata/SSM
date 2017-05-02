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
package org.apache.hadoop.ssm.actions;

/**
 * Internal actions supported.
 */
public enum ActionType {
  None(0, "None"),           // doing nothing
  External(1, "External"),   // execute some command lines specified
  CacheFile(2, "CacheFile"),
  UncacheFile(3, "UncacheFile"),
  SetStoragePolicy(4, "SetStoragePolicy"),
  EnforceStoragePolicy(5, "EnforceStoragePolicy"),
  ConvertToEC(6, "ConvertToEC"),
  ConvertToReplica(7, "ConvertToReplica"),
  Distcp(8, "Distcp"),
  DiskBalance(9, "DiskBalance"),
  BalanceCluster(10, "BalanceCluster");

  private final int value;
  private final String displayName;

  private ActionType(int value, String name) {
    this.value = value;
    this.displayName = name;
  }

  public int getValue() {
    return value;
  }

  public static ActionType fromValue(int value) {
    for (ActionType t : values()) {
      if (t.getValue() == value) {
        return t;
      }
    }
    return null;
  }

  public static ActionType fromName(String name) {
    for (ActionType t : values()) {
      if (t.getDisplayName().equalsIgnoreCase(name)) {
        return t;
      }
    }
    return null;
  }

  public String getDisplayName() {
    return displayName;
  }
}
