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
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.cmdlet.message.StatusMessage;
import org.smartdata.server.engine.cmdlet.message.StopCmdlet;
import org.smartdata.server.utils.HazelcastUtil;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class HazelcastExecutorService extends CmdletExecutorService {
  private Logger LOG = LoggerFactory.getLogger(HazelcastExecutorService.class);
  public static final String WORKER_TOPIC_PREFIX = "worker_";
  public static final String STATUS_TOPIC = "status_topic";
  private final HazelcastInstance instance;
  private Random random;
  private Map<String, ITopic<Serializable>> masterToWorkers;
  private Map<String, Set<Long>> scheduledCmdlets;
  private ITopic<StatusMessage> statusTopic;

  public HazelcastExecutorService(CmdletManager cmdletManager, CmdletFactory cmdletFactory) {
    super(cmdletManager, cmdletFactory);
    this.random = new Random();
    this.scheduledCmdlets = new HashMap<>();
    this.masterToWorkers = new HashMap<>();
    this.instance = HazelcastInstanceProvider.getInstance();
    this.statusTopic = instance.getTopic(STATUS_TOPIC);
    this.statusTopic.addMessageListener(new StatusMessageListener());
    initChannels();
    instance.getCluster().addMembershipListener(new ClusterMembershipListener(instance));
  }

  private void initChannels() {
    for (Member worker : HazelcastUtil.getWorkerMembers(this.instance)) {
      ITopic<Serializable> topic = instance.getTopic(WORKER_TOPIC_PREFIX + worker.getUuid());
      this.masterToWorkers.put(worker.getUuid(), topic);
      this.scheduledCmdlets.put(worker.getUuid(), new HashSet<Long>());
    }
  }

  @Override
  public boolean isLocalService() {
    return false;
  }

  @Override
  public boolean canAcceptMore() {
    return !HazelcastUtil.getWorkerMembers(instance).isEmpty();
  }

  @Override
  public void execute(LaunchCmdlet cmdlet) {
    String[] members = masterToWorkers.keySet().toArray(new String[0]);
    String memeber = members[random.nextInt() % members.length];
    masterToWorkers.get(memeber).publish(cmdlet);
    scheduledCmdlets.get(memeber).add(cmdlet.getCmdletId());
    LOG.info(String.format("Executing cmdlet %s on worker %s", cmdlet.getCmdletId(), members));
  }

  @Override
  public void stop(long cmdletId) {
    for (Map.Entry<String, Set<Long>> entry : scheduledCmdlets.entrySet()) {
      if (entry.getValue().contains(cmdletId)) {
        this.masterToWorkers.get(entry.getKey()).publish(new StopCmdlet(cmdletId));
      }
    }
  }

  @Override
  public void shutdown() {
  }

  public void onStatusMessage(StatusMessage message) {
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
      if (!masterToWorkers.containsKey(worker.getUuid())) {
        ITopic<Serializable> topic = instance.getTopic(WORKER_TOPIC_PREFIX + worker.getUuid());
        masterToWorkers.put(worker.getUuid(), topic);
        scheduledCmdlets.put(worker.getUuid(), new HashSet<Long>());
      }
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
      Member member = membershipEvent.getMember();
      if (masterToWorkers.containsKey(member.getUuid())) {
        masterToWorkers.get(member.getUuid()).destroy();
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
