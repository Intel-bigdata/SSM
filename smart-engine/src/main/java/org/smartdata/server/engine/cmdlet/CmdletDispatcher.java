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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.action.ActionException;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.CmdletState;
import org.smartdata.model.ExecutorType;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ActionScheduler;
import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.CmdletStatus;
import org.smartdata.server.cluster.ActiveServerNodeCmdletMetrics;
import org.smartdata.server.cluster.NodeCmdletMetrics;
import org.smartdata.server.engine.ActiveServerInfo;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.message.NodeMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
  private AtomicInteger[] execSrvSlotsLeft;
  private AtomicInteger totalSlotsLeft = new AtomicInteger();

  private Map<Long, ExecutorType> dispatchedToSrvs;
  private boolean disableLocalExec;
  private boolean logDispResult;
  private DispatchTask[] dispatchTasks;

  // TODO: to be refined
  private final int defaultSlots;
  private AtomicInteger index = new AtomicInteger(0);

  private Map<String, AtomicInteger> regNodes = new HashMap<>();
  private Map<String, NodeCmdletMetrics> regNodeInfos = new HashMap<>();

  private List<List<String>> cmdExecSrvNodeIds = new ArrayList<>();
  private String[] completeOn = new String[ExecutorType.values().length];

  public CmdletDispatcher(SmartContext smartContext, CmdletManager cmdletManager,
      Queue<Long> scheduledCmdlets, Map<Long, LaunchCmdlet> idToLaunchCmdlet,
      List<Long> runningCmdlets, ListMultimap<String, ActionScheduler> schedulers) {
    this.cmdletManager = cmdletManager;
    this.pendingCmdlets = scheduledCmdlets;
    this.runningCmdlets = runningCmdlets;
    this.idToLaunchCmdlet = idToLaunchCmdlet;
    this.schedulers = schedulers;
    int executorsNum = smartContext.getConf().getInt(SmartConfKeys.SMART_CMDLET_EXECUTORS_KEY,
        SmartConfKeys.SMART_CMDLET_EXECUTORS_DEFAULT);
    int delta = smartContext.getConf().getInt(SmartConfKeys.SMART_DISPATCH_CMDLETS_EXTRA_NUM_KEY,
        SmartConfKeys.SMART_DISPATCH_CMDLETS_EXTRA_NUM_DEFAULT);
    defaultSlots = executorsNum + delta;

    this.cmdExecServices = new CmdletExecutorService[ExecutorType.values().length];
    cmdExecSrvInsts = new int[ExecutorType.values().length];
    execSrvSlotsLeft = new AtomicInteger[ExecutorType.values().length];
    for (int i = 0; i < execSrvSlotsLeft.length; i++) {
      execSrvSlotsLeft[i] = new AtomicInteger(0);
      cmdExecSrvNodeIds.add(new ArrayList<String>());
    }
    cmdExecSrvTotalInsts = 0;
    dispatchedToSrvs = new ConcurrentHashMap<>();

    disableLocalExec = smartContext.getConf().getBoolean(
        SmartConfKeys.SMART_ACTION_LOCAL_EXECUTION_DISABLED_KEY,
        SmartConfKeys.SMART_ACTION_LOCAL_EXECUTION_DISABLED_DEFAULT);
    CmdletExecutorService exe =
        new LocalCmdletExecutorService(smartContext.getConf(), cmdletManager);
    if (!disableLocalExec) {
      registerExecutorService(exe);
    }

    SmartConf conf = smartContext.getConf();
    logDispResult = conf.getBoolean(
        SmartConfKeys.SMART_CMDLET_DISPATCHER_LOG_DISP_RESULT_KEY,
        SmartConfKeys.SMART_CMDLET_DISPATCHER_LOG_DISP_RESULT_DEFAULT);
    int numDisp = conf.getInt(SmartConfKeys.SMART_CMDLET_DISPATCHERS_KEY,
        SmartConfKeys.SMART_CMDLET_DISPATCHERS_DEFAULT);
    dispatchTasks = new DispatchTask[numDisp];
    for (int i = 0; i < numDisp; i++) {
      dispatchTasks[i] = new DispatchTask(this, i);
    }
    schExecService = Executors.newScheduledThreadPool(numDisp + 1);
  }

  public void registerExecutorService(CmdletExecutorService executorService) {
    this.cmdExecServices[executorService.getExecutorType().ordinal()] = executorService;
  }

  public boolean canDispatchMore() {
    return getTotalSlotsLeft() > 0;
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

  private void updateCmdActionStatus(LaunchCmdlet cmdlet, String host) {
    if (cmdletManager != null) {
      try {
        cmdletManager.updateCmdletExecHost(cmdlet.getCmdletId(), host);
      } catch (IOException e) {
        // Ignore this
      }
    }

    try {
      LaunchAction action;
      ActionStatus actionStatus;
      for (int i = 0; i < cmdlet.getLaunchActions().size(); i++) {
        action = cmdlet.getLaunchActions().get(i);
        actionStatus = new ActionStatus(cmdlet.getCmdletId(),
            i == cmdlet.getLaunchActions().size() - 1,
            action.getActionId(), System.currentTimeMillis());
        cmdletManager.onActionStatusUpdate(actionStatus);
      }
      CmdletStatus cmdletStatus = new CmdletStatus(cmdlet.getCmdletId(),
              System.currentTimeMillis(), CmdletState.DISPATCHED);
      cmdletManager.onCmdletStatusUpdate(cmdletStatus);
    } catch (IOException e) {
      LOG.info("update status failed.", e);
    } catch (ActionException e) {
      LOG.info("update action status failed.", e);
    }
  }

  private class DispatchTask implements Runnable {
    private final CmdletDispatcher dispatcher;
    private final int taskId;
    private int statRound = 0;
    private int statFail = 0;
    private int statDispatched = 0;
    private int statNoMoreCmdlet = 0;
    private int statFull = 0;
    private LaunchCmdlet launchCmdlet = null;

    private int[] dispInstIdxs = new int[ExecutorType.values().length];

    public DispatchTask(CmdletDispatcher dispatcher, int taskId) {
      this.dispatcher = dispatcher;
      this.taskId = taskId;
    }

    public CmdletDispatcherStat getStat() {
      CmdletDispatcherStat stat = new CmdletDispatcherStat(statRound, statFail,
          statDispatched, statNoMoreCmdlet, statFull);
      statRound = 0;
      statFail = 0;
      statDispatched = 0;
      statFull = 0;
      statNoMoreCmdlet = 0;
      return stat;
    }

    @Override
    public void run() {
      statRound++;

      if (cmdExecSrvTotalInsts == 0) {
        return;
      }

      if (!dispatcher.canDispatchMore()) {
        statFull++;
        return;
      }

      boolean redisp = launchCmdlet != null;
      boolean disped;
      while (resvExecSlot()) {
        disped = false;
        try {
          if (launchCmdlet == null) {
            launchCmdlet = getNextCmdletToRun();
          }
          if (launchCmdlet == null) {
            statNoMoreCmdlet++;
            break;
          } else {
            if (!redisp) {
              cmdletPreExecutionProcess(launchCmdlet);
            } else {
              redisp = false;
            }
            if (!dispatch(launchCmdlet)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Stop this round dispatch due : " + launchCmdlet);
              }
              statFail++;
              break;
            }
            disped = true;
            statDispatched++;
          }
        } catch (Throwable t) {
          LOG.error("Cmdlet dispatcher error", t);
        } finally {
          if (!disped) {
            freeExecSlot();
          } else {
            launchCmdlet = null;
          }
        }
      }
    }

    private boolean dispatch(LaunchCmdlet cmdlet) {
      int mod = index.incrementAndGet() % cmdExecSrvTotalInsts;
      int idx = 0;

      for (int nround = 0; nround < 2 && mod >= 0; nround++) {
        for (idx = 0; idx < cmdExecSrvInsts.length; idx++) {
          mod -= cmdExecSrvInsts[idx];
          if (mod < 0) {
            break;
          }
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          // ignore
        }
      }

      if (mod >= 0) {
        return false;
      }

      CmdletExecutorService selected = null;
      for (int i = 0; i < ExecutorType.values().length; i++) {
        idx = idx % ExecutorType.values().length;
        int left;
        do {
          left = execSrvSlotsLeft[idx].get();
          if (left > 0) {
            if (execSrvSlotsLeft[idx].compareAndSet(left, left - 1)) {
              selected = cmdExecServices[idx];
              break;
            }
          }
        } while (left > 0);

        if (selected != null) {
          break;
        }
        idx++;
      }

      if (selected == null) {
        LOG.error("No cmdlet executor service available. " + cmdlet);
        return false;
      }

      int srvId = selected.getExecutorType().ordinal();

      boolean sFlag = true;
      String nodeId;
      AtomicInteger counter;
      do {
        dispInstIdxs[srvId] = (dispInstIdxs[srvId] + 1) % cmdExecSrvNodeIds.get(srvId).size();
        nodeId = cmdExecSrvNodeIds.get(srvId).get(dispInstIdxs[srvId]);
        counter = regNodes.get(nodeId);
        int left = counter.get();
        if (left > 0) {
          if (counter.compareAndSet(left, left - 1)) {
            break;
          }
        }

        if (sFlag && completeOn[srvId] != null) {
          dispInstIdxs[srvId] = cmdExecSrvNodeIds.get(srvId).indexOf(completeOn[srvId]);
          sFlag = false;
        }
      } while (true);
      cmdlet.setNodeId(nodeId);

      boolean dispSucc = false;
      try {
        selected.execute(cmdlet);
        dispSucc = true;
      } finally {
        if (!dispSucc) {
          counter.incrementAndGet();
          execSrvSlotsLeft[idx].incrementAndGet();
        }
      }
      if (!dispSucc) {
        return false;
      }

      NodeCmdletMetrics metrics = regNodeInfos.get(nodeId);
      if (metrics != null) {
        metrics.incCmdletsInExecution();
      }
      updateCmdActionStatus(cmdlet, nodeId);
      dispatchedToSrvs.put(cmdlet.getCmdletId(), selected.getExecutorType());

      if (logDispResult) {
        LOG.info(String.format("Dispatching cmdlet->[%s] to executor: %s",
            cmdlet.getCmdletId(), nodeId));
      }
      return true;
    }
  }

  private class LogStatTask implements Runnable {
    public DispatchTask[] tasks;
    private long lastReportNoExecutor = 0;
    private long lastInfo = System.currentTimeMillis();

    public LogStatTask(DispatchTask[] tasks) {
      this.tasks = tasks;
    }

    @Override
    public void run() {
      long curr = System.currentTimeMillis();
      CmdletDispatcherStat stat = new CmdletDispatcherStat();
      for (DispatchTask task : tasks) {
        stat.add(task.getStat());
      }

      if (!(stat.getStatDispatched() == 0 && stat.getStatRound() == stat.getStatNoMoreCmdlet())) {
        if (cmdExecSrvTotalInsts != 0 || stat.getStatFull() != 0) {
          LOG.info("timeInterval={} statRound={} statFail={} statDispatched={} "
                  + "statNoMoreCmdlet={} statFull={} pendingCmdlets={} numExecutor={}",
              curr - lastInfo, stat.getStatRound(), stat.getStatFail(), stat.getStatDispatched(),
              stat.getStatNoMoreCmdlet(), stat.getStatFull(), pendingCmdlets.size(),
              cmdExecSrvTotalInsts);
        } else {
          if (curr - lastReportNoExecutor >= 600 * 1000L) {
            LOG.info("No cmdlet executor. pendingCmdlets={}", pendingCmdlets.size());
            lastReportNoExecutor = curr;
          }
        }
      }
      lastInfo = System.currentTimeMillis();
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
        LaunchCmdlet cmdlet = idToLaunchCmdlet.get(cmdletId);
        if (regNodes.get(cmdlet.getNodeId()) != null) {
          regNodes.get(cmdlet.getNodeId()).incrementAndGet();
        }

        NodeCmdletMetrics metrics = regNodeInfos.get(cmdlet.getNodeId());
        if (metrics != null) {
          metrics.finishCmdlet();
        }

        ExecutorType t = dispatchedToSrvs.remove(cmdletId);
        updateSlotsLeft(t.ordinal(), 1);
        completeOn[t.ordinal()] = cmdlet.getNodeId();
      }
    }
  }

  public void onNodeMessage(NodeMessage msg, boolean isAdd) {
    if (disableLocalExec && msg.getNodeInfo().getExecutorType() == ExecutorType.LOCAL) {
      return;
    }

    synchronized (cmdExecSrvInsts) {
      String nodeId = msg.getNodeInfo().getId();
      if (isAdd) {
        if (regNodes.containsKey(nodeId)) {
          LOG.warn("Skip duplicate add node for {}", msg.getNodeInfo());
          return;
        } else {
          regNodes.put(nodeId, new AtomicInteger(defaultSlots));
          NodeCmdletMetrics metrics;
          if (msg.getNodeInfo().getExecutorType() == ExecutorType.LOCAL) {
            metrics = new ActiveServerNodeCmdletMetrics();
          } else {
            metrics = new NodeCmdletMetrics();
          }
          metrics.setNumExecutors(defaultSlots);
          metrics.setRegistTime(System.currentTimeMillis());
          metrics.setNodeInfo(msg.getNodeInfo());
          regNodeInfos.put(nodeId, metrics);
          cmdExecSrvNodeIds.get(msg.getNodeInfo().getExecutorType().ordinal()).add(nodeId);
        }
      } else {
        if (!regNodes.containsKey(nodeId)) {
          LOG.warn("Skip duplicate remove node for {}", msg.getNodeInfo());
          return;
        } else {
          regNodes.remove(nodeId);
          regNodeInfos.remove(nodeId);
          cmdExecSrvNodeIds.get(msg.getNodeInfo().getExecutorType().ordinal()).remove(nodeId);
        }
      }

      int v = isAdd ? 1 : -1;
      int idx = msg.getNodeInfo().getExecutorType().ordinal();
      cmdExecSrvInsts[idx] += v;
      cmdExecSrvTotalInsts += v;
      updateSlotsLeft(idx, v * defaultSlots);
    }
    LOG.info(String.format("Node " + msg.getNodeInfo() + (isAdd ? " added." : " removed.")));
  }

  private void updateSlotsLeft(int idx, int delta) {
    execSrvSlotsLeft[idx].addAndGet(delta);
    totalSlotsLeft.addAndGet(delta);
  }

  public int getTotalSlotsLeft() {
    return totalSlotsLeft.get();
  }

  public boolean resvExecSlot() {
    if (totalSlotsLeft.decrementAndGet() >= 0) {
      return true;
    }
    totalSlotsLeft.incrementAndGet();
    return false;
  }

  public void freeExecSlot() {
    totalSlotsLeft.incrementAndGet();
  }

  public int getTotalSlots() {
    return cmdExecSrvTotalInsts * defaultSlots;
  }

  public Collection<NodeCmdletMetrics> getNodeCmdletMetrics() {
    ActiveServerNodeCmdletMetrics metrics = (ActiveServerNodeCmdletMetrics) regNodeInfos.get(
        ActiveServerInfo.getInstance().getId());
    if (metrics != null) {
      metrics.setNumPendingSchedule(cmdletManager.getNumPendingScheduleCmdlets());
      metrics.setNumPendingDispatch(pendingCmdlets.size());
    }
    return regNodeInfos.values();
  }

  public void start() {
    if (disableLocalExec) {
      ActiveServerNodeCmdletMetrics metrics = new ActiveServerNodeCmdletMetrics();
      metrics.setNumExecutors(defaultSlots);
      metrics.setRegistTime(System.currentTimeMillis());
      metrics.setNodeInfo(ActiveServerInfo.getInstance());
      regNodeInfos.put(ActiveServerInfo.getInstance().getId(), metrics);
    }
    CmdletDispatcherHelper.getInst().register(this);
    int idx = 0;
    for (DispatchTask task : dispatchTasks) {
      schExecService.scheduleAtFixedRate(task, idx * 200 / dispatchTasks.length,
          100, TimeUnit.MILLISECONDS);
      idx++;
    }
    schExecService.scheduleAtFixedRate(new LogStatTask(dispatchTasks),
        5000, 5000, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    CmdletDispatcherHelper.getInst().unregister();
    schExecService.shutdown();
  }
}
