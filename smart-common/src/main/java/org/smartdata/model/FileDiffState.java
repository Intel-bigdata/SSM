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

public enum FileDiffState {
  PENDING(0), // Ready for execution
  RUNNING(1), // Still running
  APPLIED(2),
  MERGED(3),
  DELETED(4),
  FAILED(5);

  private int value;

  FileDiffState(int value) {
    this.value = value;
  }

  public static FileDiffState fromValue(int value) {
    for (FileDiffState r : values()) {
      if (value == r.getValue()) {
        return r;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("FileDiffState{value=%s} %s", value, super.toString());
  }

  public static boolean isTerminalState(FileDiffState state) {
    return state.equals(APPLIED)
        || state.equals(MERGED)
        || state.equals(DELETED)
        || state.equals(FAILED);
  }
}
