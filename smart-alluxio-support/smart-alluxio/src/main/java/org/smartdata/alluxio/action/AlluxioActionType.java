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
package org.smartdata.alluxio.action;

public enum AlluxioActionType {
  None(0), // doing nothing
  External(1), // execute some cmdlet lines specified
  LOAD(2), // Load file to Alluxio Cache
  FREE(3), // Free file from alluxio
  PERSIST(4), // Persist file to under file system
  PIN(5), // Make file avoid being evicted from memory
  UNPIN(6), // Unset the PIN flag
  SetTTL(7), // Set the TTL (time to live) in milliseconds to a file
  COPY(8); // Copy file from one under file system to another

  private final int value;

  AlluxioActionType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static AlluxioActionType fromValue(int value) {
    for (AlluxioActionType t : values()) {
      if (t.getValue() == value) {
        return t;
      }
    }
    return null;
  }

  public static AlluxioActionType fromName(String name) {
    for (AlluxioActionType t : values()) {
      if (t.toString().equalsIgnoreCase(name)) {
        return t;
      }
    }
    return null;
  }

}
