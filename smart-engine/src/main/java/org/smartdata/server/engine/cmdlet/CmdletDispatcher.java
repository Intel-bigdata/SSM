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

import com.google.common.collect.ListMultimap;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.ExecutorType;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ActionScheduler;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.EngineEventBus;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.message.AddNodeMessage;
import org.smartdata.server.engine.message.NodeMessage;
import org.smartdata.server.engine.message.RemoveNodeMessage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CmdletDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(CmdletDispatcher.class);
  private Queue<Long> pendingCmdlets;
  private final CmdletManager cmdletManager;
  private final List<Long> runningCmdlets;
  private final Map<Long, LaunchCmdlet> idToLaunchCmdlet;
  private final ListMultimap<String, ActionScheduler> schedulers;

  private final ScheduledExecutorService schExecService;

  private CmdletExecutorService[] cmdExecServices;
  private int[] cmdExecSrvInsts;
  private int cmdExecSrvTotalInsts;
  private int[] cmdExecSrvInstsSlotsLeft;
  private Map<Long, ExecutorType> dispatchedToSrvs;

  // TODO: to be refined
  private final Map<String, Integer> execCmdletSlots = new ConcurrentHashMap<>();
  private final int defaultSlots;
  private int index;

  public CmdletDispatcher(SmartContext smartContext, CmdletManager cmdletManager,
      Queue<Long> scheduledCmdlets, Map<Long, LaunchCmdlet> idToLaunchCmdlet,
      List<Long> runningCmdlets, ListMultimap<String, ActionScheduler> schedulers) {
    this.cmdletManager = cmdletManager;
    this.pendingCmdlets = scheduledCmdlets;
    this.runningCmdlets = runningCmdlets;
    this.idToLaunchCmdlet = idToLaunchCmdlet;
    this.schedulers = schedulers;
    defaultSlots = smartContext.getConf().getInt(SmartConfKeys.SMART_CMDLET_EXECUTORS_KEY,
        SmartConfKeys.SMART_CMDLET_EXECUTORS_DEFAULT);

    this.cmdExecServices = new CmdletExecutorService[ExecutorType.values().length];
    cmdExecSrvInsts = new int[ExecutorType.values().length];
    cmdExecSrvTotalInsts = 0;
    cmdExecSrvInstsSlotsLeft = new int[ExecutorType.values().length];
    dispatchedToSrvs = new ConcurrentHashMap<>();
    EngineEventBus.register(this);

    boolean disableLocal = smartContext.getConf().getBoolean(
        SmartConfKeys.SMART_ACTION_LOCAL_EXECUTION_DISABLED_KEY,
        SmartConfKeys.SMART_ACTION_LOCAL_EXECUTION_DISABLED_DEFAULT);
    if (!disableLocal) {
      CmdletExecutorService exe =
          new LocalCmdletExecutorService(smartContext.getConf(), cmdletManager);
      registerExecutorService(exe);
    }
    this.index = 0;

    schExecService = Executors.newScheduledThreadPool(1);
  }

  public void registerExecutorService(CmdletExecutorService executorService) {
    this.cmdExecServices[executorService.getExecutorType().ordinal()] = executorService;
  }

  public boolean canDispatchMore() {
    return getTotalSlotsLeft() > 0;
  }

  public boolean dispatch(LaunchCmdlet cmdlet) {
    CmdletDispatchPolicy policy = cmdlet.getDispPolicy();
    if (policy == CmdletDispatchPolicy.ANY) {
      policy = getRoundrobinDispatchPolicy();
    }
    index++;
    ExecutorType[] tryOrder;
    switch (policy) {
      case PREFER_LOCAL:
        tryOrder = new ExecutorType[]
            {ExecutorType.LOCAL, ExecutorType.REMOTE_SSM, ExecutorType.AGENT};
        break;

      case PREFER_REMOTE_SSM:
        tryOrder = new ExecutorType[]
            {ExecutorType.REMOTE_SSM, ExecutorType.AGENT, ExecutorType.LOCAL};
        break;

      case PREFER_AGENT:
        tryOrder = new ExecutorType[]
            {ExecutorType.AGENT, ExecutorType.LOCAL, ExecutorType.REMOTE_SSM};
        break;

      default:
        LOG.error("Unknown cmdlet dispatch policy. " + cmdlet);
        return false;
    }

    CmdletExecutorService selected = null;
    for (ExecutorType etTry : tryOrder) {
      if (cmdExecServices[etTry.ordinal()] != null && executorSlotAvaliable(etTry)) {
        selected = cmdExecServices[etTry.ordinal()];
        break;
      }
    }

    if (selected == null) {
      LOG.error("No cmdlet executor service available. " + cmdlet);
      return false;
    }

    String id = selected.execute(cmdlet);
    updateSlotsLeft(selected.getExecutorType().ordinal(), -1);
    dispatchedToSrvs.put(cmdlet.getCmdletId(), selected.getExecutorType());

    LOG.info(
        String.format(
            "Dispatching cmdlet->[%s] to executor service %s : %s",
            cmdlet.getCmdletId(), selected.getExecutorType(), id));
    return true;
  }

  private boolean executorSlotAvaliable(ExecutorType executorType) {
    return cmdExecSrvInstsSlotsLeft[executorType.ordinal()] > 0;
  }

  private CmdletDispatchPolicy getRoundrobinDispatchPolicy() {
    int sum = 0;
    for (int v : cmdExecSrvInsts) {
      sum += v;
    }
    CmdletDispatchPolicy[] policies = new CmdletDispatchPolicy[] {
        CmdletDispatchPolicy.PREFER_LOCAL,
        CmdletDispatchPolicy.PREFER_REMOTE_SSM,
        CmdletDispatchPolicy.PREFER_AGENT
    };

    int rev = index % sum;
    for (int i = 0; i < cmdExecSrvInsts.length; i++) {
      if (cmdExecSrvInsts[i] > 0 && rev < cmdExecSrvInsts[i]) {
        return policies[i];
      } else {
        rev -= cmdExecSrvInsts[i];
      }
    }
    return policies[0]; // not reachable
  }

  //Todo: pick the right service to stop cmdlet
  public void stop(long cmdletId) {
    for (CmdletExecutorService service : cmdExecServices) {
      if (service != null) {
        service.stop(cmdletId);
      }
    }
  }

  //Todo: move this function to a proper place
  public void shutDownExcutorServices() {
    for (CmdletExecutorService service : cmdExecServices) {
      if (service != null) {
        service.shutdown();
      }
    }
  }

  public LaunchCmdlet getNextCmdletToRun() throws IOException {
    Long cmdletId = pendingCmdlets.poll();
    if (cmdletId == null) {
      return null;
    }
    LaunchCmdlet launchCmdlet = idToLaunchCmdlet.get(cmdletId);
    runningCmdlets.add(cmdletId);
    return launchCmdlet;
  }

  private void upDateCmdExecSrvInsts() {
    for (int i = 0; i < cmdExecServices.length; i++) {
      if (cmdExecServices[i] != null) {
        cmdExecSrvInsts[i] = cmdExecServices[i].getNumNodes();
      } else {
        cmdExecSrvInsts[i] = 0;
      }
    }
  }

  private class DispatchTask implements Runnable {
    private final CmdletDispatcher dispatcher;

    public DispatchTask(CmdletDispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    public void run() {
      if (!dispatcher.canDispatchMore()) {
        return;
      }

      if (cmdExecSrvTotalInsts == 0) {
        return;
      }

      LaunchCmdlet launchCmdlet;
      while (dispatcher.canDispatchMore()) {
        try {
          launchCmdlet = getNextCmdletToRun();
          if (launchCmdlet == null) {
            break;
          } else {
            cmdletPreExecutionProcess(launchCmdlet);
            if (!dispatcher.dispatch(launchCmdlet)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Stop this round dispatch due : " + launchCmdlet);
              }
              break;
            }
          }
        } catch (IOException e) {
          LOG.error("Cmdlet dispatcher error", e);
        }
      }
    }
  }

  public void cmdletPreExecutionProcess(LaunchCmdlet cmdlet) {
    for (LaunchAction action : cmdlet.getLaunchActions()) {
      for (ActionScheduler p : schedulers.get(action.getActionType())) {
        p.onPreDispatch(action);
      }
    }
  }

  public void onCmdletFinished(long cmdletId) {
    synchronized (dispatchedToSrvs) {
      if (dispatchedToSrvs.containsKey(cmdletId)) {
        ExecutorType t = dispatchedToSrvs.remove(cmdletId);
        updateSlotsLeft(t.ordinal(), 1);
      }
    }
  }

  @Subscribe
  public void onAddNodeMessage(AddNodeMessage msg) {
    onNodeMessage(msg, true);
  }

  @Subscribe
  public void onRemoveNodeMessage(RemoveNodeMessage msg) {
    onNodeMessage(msg, false);
  }

  private void onNodeMessage(NodeMessage msg, boolean isAdd) {
    synchronized (cmdExecSrvInsts) {
      int v = isAdd ? 1 : -1;
      int idx = msg.getNodeInfo().getExecutorType().ordinal();
      cmdExecSrvInsts[idx] += v;
      cmdExecSrvTotalInsts += v;
      updateSlotsLeft(idx, v * defaultSlots);
    }
    LOG.info(String.format("Node " + msg.getNodeInfo() + (isAdd ? " added." : " removed.")));
  }

  private int updateSlotsLeft(int index, int delta) {
    synchronized (cmdExecSrvInstsSlotsLeft) {
      cmdExecSrvInstsSlotsLeft[index] += delta;
      return cmdExecSrvInstsSlotsLeft[index];
    }
  }

  public int getTotalSlotsLeft() {
    synchronized (cmdExecSrvInstsSlotsLeft) {
      int total = 0;
      for (int i : cmdExecSrvInstsSlotsLeft) {
        total += i;
      }
      return total;
    }
  }

  public int getTotalSlots() {
    return cmdExecSrvTotalInsts * defaultSlots;
  }

  public void start() {
    schExecService.scheduleAtFixedRate(
        new DispatchTask(this), 200, 100, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    schExecService.shutdown();
  }
}
