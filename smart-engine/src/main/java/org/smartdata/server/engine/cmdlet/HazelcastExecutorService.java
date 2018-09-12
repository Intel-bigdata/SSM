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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.ExecutorType;
import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReport;
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.cluster.NodeInfo;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.EngineEventBus;
import org.smartdata.server.engine.StandbyServerInfo;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.cmdlet.message.StopCmdlet;
import org.smartdata.server.engine.message.AddNodeMessage;
import org.smartdata.server.engine.message.RemoveNodeMessage;
import org.smartdata.server.utils.HazelcastUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class HazelcastExecutorService extends CmdletExecutorService {
  private static final Logger LOG = LoggerFactory.getLogger(HazelcastExecutorService.class);
  public static final String WORKER_TOPIC_PREFIX = "worker_";
  public static final String STATUS_TOPIC = "status_topic";
  private final HazelcastInstance instance;
  private Random random;
  private Map<String, ITopic<Serializable>> masterToWorkers;
  private Map<Long, String> executingCmdlets;
  private Map<String, Member> members;
  private ITopic<StatusMessage> statusTopic;

  public HazelcastExecutorService(CmdletManager cmdletManager) {
    super(cmdletManager, ExecutorType.REMOTE_SSM);
    this.random = new Random();
    this.executingCmdlets = new HashMap<>();
    this.masterToWorkers = new HashMap<>();
    this.members = new HashMap<>();
    this.instance = HazelcastInstanceProvider.getInstance();
    this.statusTopic = instance.getTopic(STATUS_TOPIC);
    this.statusTopic.addMessageListener(new StatusMessageListener());
    initChannels();
    instance.getCluster().addMembershipListener(new ClusterMembershipListener(instance));
  }

  private void initChannels() {
    for (Member worker : HazelcastUtil.getWorkerMembers(instance)) {
      ITopic<Serializable> topic = instance.getTopic(WORKER_TOPIC_PREFIX + worker.getUuid());
      this.masterToWorkers.put(getMemberNodeId(worker), topic);
    }
  }

  public List<StandbyServerInfo> getStandbyServers() {
    List<StandbyServerInfo> infos = new ArrayList<>();
    for (Member worker : HazelcastUtil.getWorkerMembers(instance)) {
      infos.add(new StandbyServerInfo(getMemberNodeId(worker),
          worker.getAddress().getHost() + ":" + worker.getAddress().getPort()));
    }
    return infos;
  }

  public int getNumNodes() {
    return masterToWorkers.size();
  }

  public List<NodeInfo> getNodesInfo() {
    List<StandbyServerInfo> infos = getStandbyServers();
    List<NodeInfo> ret = new ArrayList<>(infos.size());
    for (StandbyServerInfo info : infos) {
      ret.add(info);
    }
    return ret;
  }

  private NodeInfo memberToNodeInfo(Member member) {
    return new StandbyServerInfo(getMemberNodeId(member),
        member.getAddress().getHost() + ":" + member.getAddress().getPort());
  }

  private String getMemberNodeId(Member member) {
    return "StandbySSMServer@" + member.getAddress().getHost();
  }

  @Override
  public boolean canAcceptMore() {
    return !HazelcastUtil.getWorkerMembers(instance).isEmpty();
  }

  @Override
  public String execute(LaunchCmdlet cmdlet) {
    String member = cmdlet.getNodeId();
    masterToWorkers.get(member).publish(cmdlet);
    executingCmdlets.put(cmdlet.getCmdletId(), member);
    LOG.debug("Executing cmdlet {} on worker {}", cmdlet.getCmdletId(), member);
    return member;
  }

  @Override
  public void stop(long cmdletId) {
    if (executingCmdlets.containsKey(cmdletId)) {
      String member = executingCmdlets.get(cmdletId);
      if (member != null) {
        masterToWorkers.get(member).publish(new StopCmdlet(cmdletId));
      }
    }
  }

  @Override
  public void shutdown() {
  }

  public void onStatusMessage(StatusMessage message) {
    if (message instanceof StatusReport) {
      StatusReport report = (StatusReport) message;
      for (ActionStatus s : report.getActionStatuses()) {
        if (s.isFinished() && s.isLastAction()) {
          executingCmdlets.remove(s.getCmdletId());
        }
      }
    }
    cmdletManager.updateStatus(message);
  }

  private class ClusterMembershipListener implements MembershipListener {
    private final HazelcastInstance instance;

    public ClusterMembershipListener(HazelcastInstance instance) {
      this.instance = instance;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
      Member worker = membershipEvent.getMember();
      String id = getMemberNodeId(worker);
      if (!masterToWorkers.containsKey(id)) {
        ITopic<Serializable> topic = instance.getTopic(WORKER_TOPIC_PREFIX + worker.getUuid());
        masterToWorkers.put(id, topic);
        members.put(id, worker);
        EngineEventBus.post(new AddNodeMessage(memberToNodeInfo(worker)));
      }
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
      Member member = membershipEvent.getMember();
      String id = getMemberNodeId(member);
      if (masterToWorkers.containsKey(id)) {
        masterToWorkers.get(id).destroy();
        members.remove(id);
        EngineEventBus.post(new RemoveNodeMessage(memberToNodeInfo(member)));
      }
      //Todo: recover
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
    }
  }

  private class StatusMessageListener implements MessageListener<StatusMessage> {
    @Override
    public void onMessage(Message<StatusMessage> message) {
      onStatusMessage(message.getMessageObject());
    }
  }
}
