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
package org.smartdata.server.engine.cmdlet.message;

import org.smartdata.AgentService;
import org.smartdata.model.LaunchAction;
import org.smartdata.server.engine.cmdlet.CmdletDispatchPolicy;
import org.smartdata.server.engine.cmdlet.agent.AgentCmdletService;

import java.util.List;

public class LaunchCmdlet implements AgentService.Message {
  private long cmdletId;
  private List<LaunchAction> launchActions;
  private CmdletDispatchPolicy dispPolicy = CmdletDispatchPolicy.ANY;

  public LaunchCmdlet(long cmdletId, List<LaunchAction> launchActions) {
    this.cmdletId = cmdletId;
    this.launchActions = launchActions;
  }

  public long getCmdletId() {
    return cmdletId;
  }

  public void setCmdletId(long cmdletId) {
    this.cmdletId = cmdletId;
  }

  public List<LaunchAction> getLaunchActions() {
    return launchActions;
  }

  public void setLaunchActions(List<LaunchAction> launchActions) {
    this.launchActions = launchActions;
  }

  @Override
  public String getServiceName() {
    return AgentCmdletService.NAME;
  }

  public CmdletDispatchPolicy getDispPolicy() {
    return dispPolicy;
  }

  public void setDispPolicy(CmdletDispatchPolicy dispPolicy) {
    this.dispPolicy = dispPolicy;
  }

  @Override
  public String toString() {
    return String.format("{cmdletId = %d, dispPolicy = '%s'}", cmdletId, dispPolicy);
  }
}
