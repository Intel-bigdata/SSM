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
package org.smartdata.server.cmdlet.hazelcast;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.cmdlet.CmdletFactory;
import org.smartdata.server.cmdlet.CmdletManager;
import org.smartdata.server.cmdlet.executor.CmdletExecutorService;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.cmdlet.message.StatusMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class HazelcastExecutorService extends CmdletExecutorService {
  static final String COMMAND_QUEUE = "command_queue";
  static final String STATUS_TOPIC = "status_topic";
  static final String WORKER_TO_MASTER = "worker_to_master";
  private Map<String, Set<Long>> scheduledCmdlets;
  private BlockingQueue<LaunchCmdlet> commandQueue;
  private ITopic<StatusMessage> statusTopic;
  private ITopic<HazelcastMessage> workToMaster;

  public HazelcastExecutorService(CmdletManager cmdletManager, CmdletFactory cmdletFactory) {
    super(cmdletManager, cmdletFactory);
    this.scheduledCmdlets = new HashMap<>();
    this.commandQueue = HazelcastInstanceProvider.getInstance().getQueue(COMMAND_QUEUE);
    this.statusTopic = HazelcastInstanceProvider.getInstance().getTopic(STATUS_TOPIC);
    this.statusTopic.addMessageListener(new StatusMessageListener());
    this.workToMaster = HazelcastInstanceProvider.getInstance().getTopic(WORKER_TO_MASTER);
    this.workToMaster.addMessageListener(new HazelcastMessageListener());
  }

  @Override
  public boolean isLocalService() {
    return false;
  }

  @Override
  public boolean canAcceptMore() {
    return HazelcastInstanceProvider.getInstance().getCluster().getMembers().size() > 1;
  }

  @Override
  public void execute(LaunchCmdlet cmdlet) {
    commandQueue.add(cmdlet);
  }

  private void onWorkerMessage(HazelcastMessage hazelcastMessage) {
    if (hazelcastMessage instanceof CmdletScheduled) {
      CmdletScheduled scheduled = (CmdletScheduled) hazelcastMessage;
      if (!scheduledCmdlets.containsKey(scheduled.getInstanceId())) {
        Set<Long> cmdlets = new HashSet<>();
        scheduledCmdlets.put(scheduled.getInstanceId(), cmdlets);
      }
      scheduledCmdlets.get(scheduled.getInstanceId()).add(scheduled.getCmdletId());
    }
  }

  private class HazelcastMessageListener implements MessageListener<HazelcastMessage> {
    @Override
    public void onMessage(Message<HazelcastMessage> message) {
      onWorkerMessage(message.getMessageObject());
    }
  }

  private class StatusMessageListener implements MessageListener<StatusMessage> {
    @Override
    public void onMessage(Message<StatusMessage> message) {
      cmdletManager.updateStatue(message.getMessageObject());
    }
  }
}
