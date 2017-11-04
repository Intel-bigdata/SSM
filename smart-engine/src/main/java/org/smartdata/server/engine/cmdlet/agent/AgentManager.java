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
package org.smartdata.server.engine.cmdlet.agent;

import akka.actor.ActorRef;
import org.smartdata.server.cluster.NodeInfo;
import org.smartdata.server.engine.EngineEventBus;
import org.smartdata.server.engine.cmdlet.agent.messages.MasterToAgent.AgentId;
import org.smartdata.server.engine.message.AddNodeMessage;
import org.smartdata.server.engine.message.RemoveNodeMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AgentManager {

  private final Map<ActorRef, AgentId> agents = new HashMap<>();
  private final Map<ActorRef, NodeInfo> agentNodeInfos = new HashMap<>();
  private List<ActorRef> resources = new ArrayList<>();
  private List<NodeInfo> nodeInfos = new LinkedList<>();
  private int dispatchIndex = 0;

  void addAgent(ActorRef agent, AgentId id) {
    agents.put(agent, id);
    resources.add(agent);
    String location = AgentUtils.getHostPort(agent);
    NodeInfo info = new AgentInfo(String.valueOf(id.getId()), location);
    nodeInfos.add(info);
    agentNodeInfos.put(agent, info);
    EngineEventBus.post(new AddNodeMessage(info));
  }

  AgentId removeAgent(ActorRef agent) {
    AgentId id = agents.remove(agent);
    resources.remove(agent);
    NodeInfo info = agentNodeInfos.remove(agent);
    nodeInfos.remove(info);
    EngineEventBus.post(new RemoveNodeMessage(info));
    return id;
  }

  boolean hasFreeAgent() {
    return !resources.isEmpty();
  }

  ActorRef dispatch() {
    int id = dispatchIndex % resources.size();
    dispatchIndex++;
    return resources.get(id);
  }

  Map<ActorRef, AgentId> getAgents() {
    return agents;
  }

  List<NodeInfo> getNodeInfos() {
    return nodeInfos;
  }

  AgentId getAgentId(ActorRef agentActorRef) {
    return agents.get(agentActorRef);
  }
}
