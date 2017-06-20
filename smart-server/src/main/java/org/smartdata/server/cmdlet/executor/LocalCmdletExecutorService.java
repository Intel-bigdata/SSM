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
package org.smartdata.server.cmdlet.executor;

import org.smartdata.server.cmdlet.CmdletFactory;
import org.smartdata.server.cmdlet.CmdletManager;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.cmdlet.message.StatusMessage;

public class LocalCmdletExecutorService extends CmdletExecutorService implements CmdletStatusReporter {
  private CmdletExecutor cmdletExecutor;

  public LocalCmdletExecutorService(CmdletManager cmdletManager, CmdletFactory cmdletFactory) {
    super(cmdletManager, cmdletFactory);
    this.cmdletExecutor = new CmdletExecutor(this);
  }

  @Override
  public boolean isLocalService() {
    return true;
  }

  @Override
  public boolean canAcceptMore() {
    return true;
  }

  @Override
  public void execute(LaunchCmdlet cmdlet) {
    this.cmdletExecutor.execute(cmdletFactory.createCmdlet(cmdlet));
  }

  @Override
  public void report(StatusMessage status) {
    cmdletManager.updateStatue(status);
  }
}
