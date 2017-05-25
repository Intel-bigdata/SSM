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
package org.apache.hadoop.smart.actions;

/**
 * Internal actions supported.
 */
public enum ActionType {
  None(0),              // doing nothing
  External(1),          // execute some command lines specified
  CacheFile(2),         // Move to cache
  UncacheFile(3),       // Move out of cache
  SetStoragePolicy(4),  // Set Policy Action
  MoveFile(5),          // Enforce storage Policy
  ArchiveFile(6),       // Enforce Archive Policy
  ConvertToEC(7),
  ConvertToReplica(8),
  Distcp(9),
  DiskBalance(10),
  BalanceCluster(11);

  private final int value;

  ActionType(int value) {
    this.value = value;
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
      if (t.toString().equalsIgnoreCase(name)) {
        return t;
      }
    }
    return null;
  }
}
