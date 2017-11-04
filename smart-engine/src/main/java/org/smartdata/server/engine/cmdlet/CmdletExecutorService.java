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
package org.smartdata.server.engine.cmdlet;

import org.smartdata.model.ExecutorType;
import org.smartdata.server.cluster.NodeInfo;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;

import java.util.List;

public abstract class CmdletExecutorService {
  protected CmdletManager cmdletManager;
  private ExecutorType executorType;

  public CmdletExecutorService(CmdletManager cmdletManager, ExecutorType executorType) {
    this.cmdletManager = cmdletManager;
    this.executorType = executorType;
  }

  public abstract boolean canAcceptMore();

  // TODO: to be refined
  /**
   * Send cmdlet to end executor for execution.
   *
   * @param cmdlet
   * @return Node ID that the cmdlet been dispatched to, null if failed
   */
  public abstract String execute(LaunchCmdlet cmdlet);

  public abstract void stop(long cmdletId);

  public abstract void shutdown();

  public ExecutorType getExecutorType() {
    return executorType;
  }

  public abstract int getNumNodes();  // return number of nodes contained

  public abstract List<NodeInfo> getNodesInfo();
}
