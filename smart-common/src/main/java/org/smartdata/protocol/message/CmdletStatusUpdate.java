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
package org.smartdata.protocol.message;

import org.smartdata.model.CmdletState;

public class CmdletStatusUpdate implements StatusMessage {
  private long cmdletId;
  private long timestamp;
  private CmdletState currentState;

  public CmdletStatusUpdate(long cmdletId, long timestamp, CmdletState currentState) {
    this.cmdletId = cmdletId;
    this.timestamp = timestamp;
    this.currentState = currentState;
  }

  public long getCmdletId() {
    return cmdletId;
  }

  public void setCmdletId(long cmdletId) {
    this.cmdletId = cmdletId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public CmdletState getCurrentState() {
    return currentState;
  }

  public void setCurrentState(CmdletState currentState) {
    this.currentState = currentState;
  }

  @Override
  public String toString() {
    return String.format("Cmdlet %s transfers to status %s", cmdletId, currentState);
  }
}
