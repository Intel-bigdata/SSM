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

import java.io.Serializable;

public class CmdletStatus implements Serializable{
  private long cmdletId;
  private long stateUpdateTime;
  private CmdletState currentState;

  public CmdletStatus(long cmdletId, long stateUpdateTime, CmdletState state) {
    this.cmdletId = cmdletId;
    this.stateUpdateTime = stateUpdateTime;
    this.currentState = state;
  }

  public long getCmdletId() {
    return cmdletId;
  }

  public long getStateUpdateTime() {
    return stateUpdateTime;
  }

  public CmdletState getCurrentState() {
    return currentState;
  }
}
