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

import org.smartdata.conf.SmartConf;
import org.smartdata.model.ExecutorType;
import org.smartdata.server.cluster.NodeInfo;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.CmdletExecutorService;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AgentExecutorService extends CmdletExecutorService {

  private AgentMaster master;

  public AgentExecutorService(SmartConf conf, CmdletManager cmdletManager) throws IOException {
    super(cmdletManager, ExecutorType.AGENT);
    master = AgentMaster.getAgentMaster(conf);
    AgentMaster.setCmdletManager(cmdletManager);
  }

  @Override
  public boolean canAcceptMore() {
    return master.canAcceptMore();
  }

  @Override
  public String execute(LaunchCmdlet cmdlet) {
    return master.launchCmdlet(cmdlet);
  }

  @Override
  public void stop(long cmdletId) {
    master.stopCmdlet(cmdletId);
  }

  @Override
  public void shutdown() {
    master.shutdown();
  }

  public List<AgentInfo> getAgentInfos() {
    return master.getAgentInfos();
  }

  public int getNumNodes() {
    return master.getNumAgents();
  }

  public List<NodeInfo> getNodesInfo() {
    List<AgentInfo> infos = getAgentInfos();
    List<NodeInfo> ret = new ArrayList<>(infos.size());
    for (AgentInfo info : infos) {
      ret.add(info);
    }
    return ret;
  }
}
