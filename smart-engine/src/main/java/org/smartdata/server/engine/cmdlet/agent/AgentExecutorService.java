/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.server.engine.cmdlet.agent;

import org.smartdata.server.engine.cmdlet.CmdletFactory;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.CmdletExecutorService;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;

public class AgentExecutorService extends CmdletExecutorService {

  private AgentMaster master;

  public AgentExecutorService(CmdletManager cmdletManager) {
    super(cmdletManager);
    this.master = new AgentMaster(cmdletManager);
  }

  @Override
  public boolean isLocalService() {
    return false;
  }

  @Override
  public boolean canAcceptMore() {
    return master.canAcceptMore();
  }

  @Override
  public void execute(LaunchCmdlet cmdlet) {
    master.launchCmdlet(cmdlet);
  }

  @Override
  public void stop(long cmdletId) {
    master.stopCmdlet(cmdletId);
  }

  @Override
  public void shutdown() {
    master.shutdown();
  }
}
