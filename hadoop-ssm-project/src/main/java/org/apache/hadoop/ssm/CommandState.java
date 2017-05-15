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
package org.apache.hadoop.ssm;

/**
 * The possible state that a command can be in.
 */
public enum CommandState {
  NOTINITED(0),
  PENDING(1), // Ready for execution
  EXECUTING(2), // Still running
  PAUSED(3),
  DONE(4), // Execution successful
  CANCELLED(5);

  private int value;

  private CommandState(int value) {
    this.value = value;
  }
  public static CommandState fromValue(int value) {
    for (CommandState r : values()) {
      if (value == r.getValue()) {
        return r;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }
}
