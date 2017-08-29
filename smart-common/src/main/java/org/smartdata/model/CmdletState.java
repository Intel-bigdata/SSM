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

/**
 * The possible state that a cmdlet can be in.
 */
public enum CmdletState {
  NOTINITED(0),
  PENDING(1), // Ready for schedule
  EXECUTING(2), // Still running
  PAUSED(3),
  DONE(4), // Execution successful
  CANCELLED(5),
  DISABLED(6), // Disable this Cmdlet, kill all executing actions
  DRYRUN(7),   // TODO Don't Run, but keep status
  FAILED(8),   // Running cmdlet failed
  SCHEDULED(9);

  private int value;

  private CmdletState(int value) {
    this.value = value;
  }

  public static CmdletState fromValue(int value) {
    for (CmdletState r : values()) {
      if (value == r.getValue()) {
        return r;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }

  public static boolean isTerminalState(CmdletState state) {
    return state.equals(DONE)
        || state.equals(CANCELLED)
        || state.equals(DISABLED)
        || state.equals(FAILED);
  }

  @Override
  public String toString() {
    return String.format("CmdletState{value=%s} %s", value, super.toString());
  }
}
