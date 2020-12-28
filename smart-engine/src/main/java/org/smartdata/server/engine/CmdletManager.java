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
package org.smartdata.server.engine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.action.ActionException;
import org.smartdata.action.ActionRegistry;
import org.smartdata.action.SmartAction;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.exception.QueueFullException;
import org.smartdata.hdfs.action.move.AbstractMoveFileAction;
import org.smartdata.hdfs.scheduler.ActionSchedulerService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.DetailedFileAction;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.UserInfo;
import org.smartdata.model.WhitelistHelper;
import org.smartdata.model.action.ActionScheduler;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.ActionStatusFactory;
import org.smartdata.protocol.message.CmdletStatus;
import org.smartdata.protocol.message.CmdletStatusUpdate;
import org.smartdata.protocol.message.LaunchCmdlet;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.protocol.message.StatusReport;
import org.smartdata.server.cluster.ActiveServerNodeCmdletMetrics;
import org.smartdata.server.cluster.NodeCmdletMetrics;
import org.smartdata.server.engine.cmdlet.CmdletDispatcher;
import org.smartdata.server.engine.cmdlet.CmdletExecutorService;
import org.smartdata.server.engine.cmdlet.TaskTracker;
import org.smartdata.utils.StringUtil;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * When a Cmdlet is submitted, it's string descriptor will be stored into set submittedCmdlets
 * to avoid duplicated Cmdlet, then enqueue into pendingCmdlet. When the Cmdlet is scheduled it
 * will be remove out of the queue and marked in the runningCmdlets.
 *
 * <p>The map idToCmdlets stores all the recent CmdletInfos, including pending and running Cmdlets.
 * After the Cmdlet is finished or cancelled or failed, it's status will be flush to DB.
 */
public class CmdletManager extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(CmdletManager.class);
  public static final int TIMEOUT_MULTIPLIER = 100;
  public static final int TIMEOUT_MIN_MILLISECOND = 30000;

  private ScheduledExecutorService executorService;
  private CmdletDispatcher dispatcher;
  private MetaStore metaStore;
  private AtomicLong maxActionId;
  private AtomicLong maxCmdletId;
  // cache sync threshold
  private int cacheCmdTh;

  private int maxNumPendingCmdlets;
  private List<Long> pendingCmdlet;
  private List<Long> schedulingCmdlet;
  private Queue<Long> scheduledCmdlet;
  private Map<Long, LaunchCmdlet> idToLaunchCmdlet;
  private List<Long> runningCmdlets;
  private Map<Long, CmdletInfo> idToCmdlets;
  // Track a CmdletDescriptor from the submission to
  // the finish.
  private TaskTracker tracker;
  private Map<Long, ActionInfo> idToActions;
  private Map<Long, CmdletInfo> cacheCmd;
  private List<Long> tobeDeletedCmd;
  private ListMultimap<String, ActionScheduler> schedulers = ArrayListMultimap.create();
  private List<ActionSchedulerService> schedulerServices = new ArrayList<>();

  private AtomicLong numCmdletsGen = new AtomicLong(0);
  private AtomicLong numCmdletsFinished = new AtomicLong(0);

  private long totalScheduled = 0;

  private ActionGroup tmpActions = new ActionGroup();

  private long timeout;

  private ActionGroup cache;

  public CmdletManager(ServerContext context) throws IOException {
    super(context);

    this.metaStore = context.getMetaStore();
    this.executorService = Executors.newScheduledThreadPool(4);
    this.runningCmdlets = new ArrayList<>();
    this.pendingCmdlet = new LinkedList<>();
    this.schedulingCmdlet = new LinkedList<>();
    this.scheduledCmdlet = new LinkedBlockingQueue<>();
    this.idToLaunchCmdlet = new ConcurrentHashMap<>();
    this.idToCmdlets = new ConcurrentHashMap<>();
    this.tracker = new TaskTracker();
    this.idToActions = new ConcurrentHashMap<>();
    this.cacheCmd = new ConcurrentHashMap<>();
    this.tobeDeletedCmd = new LinkedList<>();
    this.dispatcher = new CmdletDispatcher(context, this, scheduledCmdlet,
      idToLaunchCmdlet, runningCmdlets, schedulers);
    maxNumPendingCmdlets = context.getConf()
      .getInt(SmartConfKeys.SMART_CMDLET_MAX_NUM_PENDING_KEY,
        SmartConfKeys.SMART_CMDLET_MAX_NUM_PENDING_DEFAULT);
    cacheCmdTh = context.getConf()
      .getInt(SmartConfKeys.SMART_CMDLET_CACHE_BATCH,
        SmartConfKeys.SMART_CMDLET_CACHE_BATCH_DEFAULT);

    int reportPeriod = context.getConf().getInt(SmartConfKeys.SMART_STATUS_REPORT_PERIOD_KEY,
        SmartConfKeys.SMART_STATUS_REPORT_PERIOD_DEFAULT);
    int maxInterval = reportPeriod * context.getConf().getInt(
        SmartConfKeys.SMART_STATUS_REPORT_PERIOD_MULTIPLIER_KEY,
        SmartConfKeys.SMART_STATUS_REPORT_PERIOD_MULTIPLIER_DEFAULT);
    this.timeout = TIMEOUT_MULTIPLIER * maxInterval < TIMEOUT_MIN_MILLISECOND
        ? TIMEOUT_MIN_MILLISECOND : TIMEOUT_MULTIPLIER * maxInterval;
  }

  @VisibleForTesting
  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  @VisibleForTesting
  void setDispatcher(CmdletDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void init() throws IOException {
    LOG.info("Initializing ...");
    try {
      maxActionId = new AtomicLong(metaStore.getMaxActionId());
      maxCmdletId = new AtomicLong(metaStore.getMaxCmdletId());
      numCmdletsFinished.addAndGet(metaStore.getNumCmdletsInTerminiatedStates());

      schedulerServices = AbstractServiceFactory.createActionSchedulerServices(
        getContext().getConf(), (ServerContext) getContext(), metaStore, false);

      for (ActionSchedulerService s : schedulerServices) {
        s.init();
        List<String> actions = s.getSupportedActions();
        for (String a : actions) {
          schedulers.put(a, s);
        }
      }
      recover();
      LOG.info("Initialized.");
    } catch (MetaStoreException e) {
      LOG.error("DB Connection error! Failed to get Max CmdletId/ActionId!", e);
      throw new IOException(e);
    } catch (IOException e) {
      throw e;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  private void recover() throws IOException {
    reloadCmdletsInDB();
  }

  @VisibleForTesting
  public List<ActionScheduler> getSchedulers(String actionName) {
    return schedulers.get(actionName);
  }

  private void reloadCmdletsInDB() throws IOException{
    LOG.info("reloading the dispatched and pending cmdlets in DB.");
    List<CmdletInfo> cmdletInfos;
    try {
      cmdletInfos = metaStore.getCmdlets(CmdletState.DISPATCHED);
      if (cmdletInfos != null && cmdletInfos.size() != 0) {
        for (CmdletInfo cmdletInfo : cmdletInfos) {
          List<ActionInfo> actionInfos = getActions(cmdletInfo.getAids());
          for (ActionInfo actionInfo: actionInfos) {
            actionInfo.setCreateTime(cmdletInfo.getGenerateTime());
            actionInfo.setFinishTime(System.currentTimeMillis());
          }
          syncCmdAction(cmdletInfo, actionInfos);
        }
      }

      cmdletInfos = metaStore.getCmdlets(CmdletState.PENDING);
      if (cmdletInfos != null && cmdletInfos.size() != 0) {
        for (CmdletInfo cmdletInfo : cmdletInfos) {
          LOG.debug(String.format("Reload pending cmdlet: {}", cmdletInfo));
          List<ActionInfo> actionInfos = getActions(cmdletInfo.getAids());
          syncCmdAction(cmdletInfo, actionInfos);
        }
      }
    } catch (MetaStoreException e) {
      LOG.error("DB connection error occurs when ssm is reloading cmdlets!");
      return;
    }
  }

  /**
   * Check if action names in cmdletDescriptor are correct.
   * @param cmdletDescriptor
   * @throws IOException
   */
  private void checkActionNames(
    CmdletDescriptor cmdletDescriptor) throws IOException {
    for (int index = 0; index < cmdletDescriptor.getActionSize(); index++) {
      if (!ActionRegistry
        .registeredAction(cmdletDescriptor.getActionName(index))) {
        throw new IOException(
          String.format(
            "Submit Cmdlet %s error! Action names are not correct!",
            cmdletDescriptor));
      }
    }
  }

  /**
   * Let Scheduler check actioninfo onsubmit and add them to cmdletinfo.
   * TODO: remove useless actionIndex.
   * @param cmdletInfo
   * @param actionInfos
   * @throws IOException
   */
  private void checkActionsOnSubmit(CmdletInfo cmdletInfo,
      List<ActionInfo> actionInfos) throws IOException {
    for (ActionInfo actionInfo : actionInfos) {
      cmdletInfo.addAction(actionInfo.getActionId());
    }
    int actionIndex = 0;
    for (ActionInfo actionInfo : actionInfos) {
      for (ActionScheduler p : schedulers.get(actionInfo.getActionName())) {
        if (!p.onSubmit(cmdletInfo, actionInfo, actionIndex)) {
          throw new IOException(
            String.format("Action rejected by scheduler", actionInfo));
        }
      }
      actionIndex++;
    }
  }

  @Override
  public void start() throws IOException {
    LOG.info("Starting ...");
    executorService.scheduleAtFixedRate(new CmdletPurgeTask(getContext().getConf()),
        10, 5000, TimeUnit.MILLISECONDS);
    executorService.scheduleAtFixedRate(new ScheduleTask(), 100, 50, TimeUnit.MILLISECONDS);
    executorService.scheduleAtFixedRate(new FlushCachedCmdletsTask(), 200, 50,
        TimeUnit.MILLISECONDS);
    executorService.scheduleAtFixedRate(new DetectFailedActionTask(), 1000, 5000,
        TimeUnit.MILLISECONDS);

    for (ActionSchedulerService s : schedulerServices) {
      s.start();
    }
    dispatcher.start();
    LOG.info("Started.");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");
    dispatcher.stop();
    for (int i = schedulerServices.size() - 1; i >= 0; i--) {
      schedulerServices.get(i).stop();
    }
    executorService.shutdown();
    cacheCmdTh = Integer.MAX_VALUE;
    try {
      batchSyncCmdAction();
    } catch (Exception e) {
      throw new IOException(e);
    }
    dispatcher.shutDownExcutorServices();
    LOG.info("Stopped.");
  }

  /**
   * Register agentExecutorService & hazelcastExecutorService.
   */
  public void registerExecutorService(CmdletExecutorService executorService) {
    dispatcher.registerExecutorService(executorService);
  }

  public void addNewUser(UserInfo userInfo) throws MetaStoreException {
    metaStore.insertUserInfo(userInfo);
  }

  public void newPassword(
      UserInfo userInfo) throws MetaStoreException {
    try {
      metaStore.newPassword(userInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  /**
   * Compare userInfo(userName, password) with the one recorded in metastore.
   * @param userInfo  its password should be encrypted by SHA512.
   * @return  true if the given user info equals the one recorded in metastore.
   * @throws MetaStoreException
   */
  public boolean authentic (UserInfo userInfo) throws MetaStoreException {
    try {
      UserInfo origin = metaStore.getUserInfoByUserName(userInfo.getUserName());
      if (origin == null) {
        LOG.warn("The given user is not registered: " + userInfo.getUserName());
        return false;
      }
      return origin.equals(userInfo);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public long submitCmdlet(String cmdlet) throws IOException {
    LOG.debug(String.format("Received Cmdlet -> [ %s ]", cmdlet));
    try {
      if (StringUtils.isBlank(cmdlet)) {
        throw new IOException("Cannot submit an empty action!");
      }
      CmdletDescriptor cmdletDescriptor = CmdletDescriptor.fromCmdletString(cmdlet);
      return submitCmdlet(cmdletDescriptor);
    } catch (ParseException e) {
      LOG.debug("Cmdlet -> [ {} ], format is not correct", cmdlet, e);
      throw new IOException(e);
    }
  }

  public long submitCmdlet(CmdletDescriptor cmdletDescriptor) throws IOException {
    // To avoid repeatedly submitting task. If tracker contains one CmdletDescriptor
    // with the same rule id and cmdlet string, return -1.
    if (tracker.contains(cmdletDescriptor)) {
      LOG.debug("Refuse to repeatedly submit Cmdlet for {}", cmdletDescriptor);
      return -1;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Received Cmdlet -> [ %s ]", cmdletDescriptor.getCmdletString()));
    }
    if (maxNumPendingCmdlets <= pendingCmdlet.size() + schedulingCmdlet.size()) {
      throw new QueueFullException("Pending cmdlets exceeds value specified by key '"
          + SmartConfKeys.SMART_CMDLET_MAX_NUM_PENDING_KEY + "' = " + maxNumPendingCmdlets);
    }
    long submitTime = System.currentTimeMillis();
    CmdletInfo cmdletInfo =
      new CmdletInfo(
        maxCmdletId.getAndIncrement(),
        cmdletDescriptor.getRuleId(),
        CmdletState.PENDING,
        cmdletDescriptor.getCmdletString(),
        submitTime,
        submitTime,
        submitTime + cmdletDescriptor.getDeferIntervalMs());
    List<ActionInfo> actionInfos =
        createActionInfos(cmdletDescriptor, cmdletInfo.getCid());
    // Check action names
    checkActionNames(cmdletDescriptor);
    // Check if action path is in whitelist
    if (WhitelistHelper.isEnabled(getContext().getConf())) {
      if (!WhitelistHelper.isCmdletInWhitelist(cmdletDescriptor)) {
        throw new IOException("This path is not in the whitelist.");
      }
    }
    // Let Scheduler check actioninfo onsubmit and add them to cmdletinfo
    checkActionsOnSubmit(cmdletInfo, actionInfos);
    // Insert cmdletinfo and actionInfos to metastore and cache.
    syncCmdAction(cmdletInfo, actionInfos);
    // Track in the submission portal. For cmdlets recovered from DB
    // (see #recover), they will be not be tracked.
    tracker.track(cmdletInfo.getCid(), cmdletDescriptor);
    return cmdletInfo.getCid();
  }

  /**
   * Insert cmdletinfo and actions to metastore and cache.
   *
   * @param cmdletInfo
   * @param actionInfos
   * @throws IOException
   */
  private void syncCmdAction(CmdletInfo cmdletInfo,
      List<ActionInfo> actionInfos) throws IOException {
    LOG.debug("Cache cmd {}", cmdletInfo);
    for (ActionInfo actionInfo : actionInfos) {
      idToActions.put(actionInfo.getActionId(), actionInfo);
    }
    idToCmdlets.put(cmdletInfo.getCid(), cmdletInfo);

    if (cmdletInfo.getState() == CmdletState.PENDING) {
      numCmdletsGen.incrementAndGet();
      cacheCmd.put(cmdletInfo.getCid(), cmdletInfo);
      synchronized (pendingCmdlet) {
        pendingCmdlet.add(cmdletInfo.getCid());
      }
    } else if (cmdletInfo.getState() == CmdletState.DISPATCHED) {
      runningCmdlets.add(cmdletInfo.getCid());
      LaunchCmdlet launchCmdlet = createLaunchCmdlet(cmdletInfo);
      idToLaunchCmdlet.put(cmdletInfo.getCid(), launchCmdlet);
    }
  }

  private void batchSyncCmdAction() throws Exception {
    if (cacheCmd.size() == 0 && tobeDeletedCmd.size() == 0) {
      return;
    }
    List<CmdletInfo> cmdletInfos = new ArrayList<>();
    List<ActionInfo> actionInfos = new ArrayList<>();
    List<CmdletInfo> cmdletFinished = new ArrayList<>();
    LOG.debug("Number of cached cmds {}", cacheCmd.size());
    int todelSize;
    synchronized (cacheCmd) {
      synchronized (tobeDeletedCmd) {
        todelSize = tobeDeletedCmd.size();
        for (Long cid : tobeDeletedCmd) {
          cacheCmd.remove(cid);
        }
      }
      for (Long cid : cacheCmd.keySet()) {
        CmdletInfo cmdletInfo = cacheCmd.remove(cid);
        if (cmdletInfo.getState() != CmdletState.DISABLED) {
          cmdletInfos.add(cmdletInfo);
          for (Long aid : cmdletInfo.getAids()) {
            ActionInfo actionInfo = idToActions.get(aid);
            if (actionInfo != null) {
              actionInfos.add(actionInfo);
            }
          }
        }
        if (CmdletState.isTerminalState(cmdletInfo.getState())) {
          cmdletFinished.add(cmdletInfo);
        }
        if (cmdletInfos.size() >= cacheCmdTh) {
          break;
        }
      }

      for (CmdletInfo cmdletInfo : cmdletFinished) {
        idToCmdlets.remove(cmdletInfo.getCid());
        tracker.untrack(cmdletInfo.getCid());
        for (Long aid : cmdletInfo.getAids()) {
          idToActions.remove(aid);
        }
      }
    }

    if (cmdletInfos.size() > 0) {
      LOG.debug("Number of cmds {} to submit", cmdletInfos.size());
      try {
        metaStore.insertActions(
            actionInfos.toArray(new ActionInfo[actionInfos.size()]));
        metaStore.insertCmdlets(
            cmdletInfos.toArray(new CmdletInfo[cmdletInfos.size()]));
      } catch (MetaStoreException e) {
        LOG.error("CmdletIds -> [ {} ], submit to DB error", cmdletInfos, e);
      }
    }

    if (todelSize > 0) {
      List<Long> del = new LinkedList<>();
      synchronized (tobeDeletedCmd) {
        del.addAll(tobeDeletedCmd.subList(0, todelSize > cacheCmdTh ? cacheCmdTh : todelSize));
        tobeDeletedCmd.removeAll(del);
      }

      if (del.size() > 0) {
        LOG.debug("Number of cmds {} to delete", del.size());
        try {
          metaStore.batchDeleteCmdlet(del);
          metaStore.batchDeleteCmdletActions(del);
        } catch (MetaStoreException e) {
          LOG.error("CmdletIds -> [ {} ], delete from DB error", del, e);
        }
      }
    }
  }

  private boolean shouldStopSchedule() {
    int left = dispatcher.getTotalSlotsLeft();
    int total = dispatcher.getTotalSlots();
    if (scheduledCmdlet.size() >= left + total * 0.2) {
      return true;
    }
    return false;
  }

  private int getNumPendingScheduleCmdlets() {
    return pendingCmdlet.size() + schedulingCmdlet.size();
  }

  public void updateNodeCmdletMetrics(ActiveServerNodeCmdletMetrics metrics) {
    metrics.setMaxPendingSchedule(maxNumPendingCmdlets);
    metrics.setNumPendingSchedule(getNumPendingScheduleCmdlets());
  }

  public Collection<NodeCmdletMetrics> getAllNodeCmdletMetrics() {
    return dispatcher.getNodeCmdletMetrics();
  }

  private int scheduleCmdlet() throws IOException {
    int nScheduled = 0;

    synchronized (pendingCmdlet) {
      if (pendingCmdlet.size() > 0) {
        schedulingCmdlet.addAll(pendingCmdlet);
        pendingCmdlet.clear();
      }
    }

    long curr = System.currentTimeMillis();
    Iterator<Long> it = schedulingCmdlet.iterator();
    while (it.hasNext() && !shouldStopSchedule()) {
      long id = it.next();
      if (nScheduled % 20 == 0) {
        curr = System.currentTimeMillis();
      }
      CmdletInfo cmdlet = idToCmdlets.get(id);
      if (cmdlet == null) {
        it.remove();
        continue;
      }

      synchronized (cmdlet) {
        switch (cmdlet.getState()) {
          case CANCELLED:
          case DISABLED:
            it.remove();
            break;

          case PENDING:
            if (cmdlet.getDeferedToTime() > curr) {
              break;
            }

            LaunchCmdlet launchCmdlet = createLaunchCmdlet(cmdlet);
            ScheduleResult result;
            try {
              result = scheduleCmdletActions(cmdlet, launchCmdlet);
            } catch (Throwable t) {
              LOG.error("Schedule " + cmdlet + " failed.", t);
              result = ScheduleResult.FAIL;
            }
            if (result != ScheduleResult.RETRY) {
              it.remove();
            } else {
              continue;
            }
            try {
              if (result == ScheduleResult.SUCCESS) {
                idToLaunchCmdlet.put(cmdlet.getCid(), launchCmdlet);
                cmdlet.setState(CmdletState.SCHEDULED);
                cmdlet.setStateChangedTime(System.currentTimeMillis());
                scheduledCmdlet.add(id);
                nScheduled++;
              } else if (result == ScheduleResult.FAIL) {
                cmdlet.updateState(CmdletState.CANCELLED);
                CmdletStatus cmdletStatus = new CmdletStatus(
                    cmdlet.getCid(), cmdlet.getStateChangedTime(), cmdlet.getState());
                // Mark all actions as finished
                cmdletFinishedInternal(cmdlet, false);
                onCmdletStatusUpdate(cmdletStatus);
              } else if (result == ScheduleResult.SUCCESS_NO_EXECUTION) {
                cmdlet.updateState(CmdletState.DONE);
                cmdletFinishedInternal(cmdlet, true);
                CmdletStatus cmdletStatus = new CmdletStatus(
                    cmdlet.getCid(), cmdlet.getStateChangedTime(), cmdlet.getState());
                onCmdletStatusUpdate(cmdletStatus);
              }
            } catch (Throwable t) {
              LOG.error("Post schedule cmdlet " + cmdlet + " error.", t);
            }
            break;
        }
      }
    }
    return nScheduled;
  }

  private ScheduleResult scheduleCmdletActions(CmdletInfo info,
      LaunchCmdlet launchCmdlet) {
    List<Long> actIds = info.getAids();
    int idx = 0;
    int schIdx = 0;
    ActionInfo actionInfo;
    LaunchAction launchAction;
    List<ActionScheduler> actSchedulers;
    boolean skipped = false;
    ScheduleResult scheduleResult = ScheduleResult.SUCCESS_NO_EXECUTION;
    ScheduleResult resultTmp;
    for (idx = 0; idx < actIds.size(); idx++) {
      actionInfo = idToActions.get(actIds.get(idx));
      launchAction = launchCmdlet.getLaunchActions().get(idx);
      actSchedulers = schedulers.get(actionInfo.getActionName());
      if (actSchedulers == null || actSchedulers.size() == 0) {
        skipped = true;
        continue;
      }

      for (schIdx = 0; schIdx < actSchedulers.size(); schIdx++) {
        ActionScheduler s = actSchedulers.get(schIdx);
        try {
          resultTmp = s.onSchedule(info, actionInfo, launchCmdlet, launchAction, idx);
        } catch (Throwable t) {
          actionInfo.appendLogLine("\nOnSchedule exception: " + t);
          resultTmp = ScheduleResult.FAIL;
        }

        if (resultTmp != ScheduleResult.SUCCESS
            && resultTmp != ScheduleResult.SUCCESS_NO_EXECUTION) {
          scheduleResult = resultTmp;
        } else {
          if (scheduleResult == ScheduleResult.SUCCESS_NO_EXECUTION) {
            scheduleResult = resultTmp;
          }
        }

        if (scheduleResult != ScheduleResult.SUCCESS
            && scheduleResult != ScheduleResult.SUCCESS_NO_EXECUTION) {
          break;
        }
      }

      if (scheduleResult != ScheduleResult.SUCCESS
          && scheduleResult != ScheduleResult.SUCCESS_NO_EXECUTION) {
        break;
      }
    }

    if (scheduleResult == ScheduleResult.SUCCESS
        || scheduleResult == ScheduleResult.SUCCESS_NO_EXECUTION) {
      idx--;
      schIdx--;
      if (skipped) {
        scheduleResult = ScheduleResult.SUCCESS;
      }
    }
    postscheduleCmdletActions(info, actIds, scheduleResult, idx, schIdx);
    return scheduleResult;
  }

  private void postscheduleCmdletActions(CmdletInfo cmdletInfo,
      List<Long> actions, ScheduleResult result,
      int lastAction, int lastScheduler) {
    List<ActionScheduler> actSchedulers;
    for (int aidx = lastAction; aidx >= 0; aidx--) {
      ActionInfo info = idToActions.get(actions.get(aidx));
      actSchedulers = schedulers.get(info.getActionName());
      if (actSchedulers == null || actSchedulers.size() == 0) {
        continue;
      }
      if (lastScheduler < 0) {
        lastScheduler = actSchedulers.size() - 1;
      }

      for (int sidx = lastScheduler; sidx >= 0; sidx--) {
        try {
          actSchedulers.get(sidx).postSchedule(cmdletInfo, info, sidx, result);
        } catch (Throwable t) {
          info.setLog((info.getLog() == null ? "" : info.getLog())
              + "\nPostSchedule exception: " + t);
        }
      }

      lastScheduler = -1;
    }
  }

  private LaunchCmdlet createLaunchCmdlet(CmdletInfo cmdletInfo) {
    if (cmdletInfo == null) {
      return null;
    }
    Map<String, String> args;
    List<LaunchAction> launchActions = new ArrayList<>();
    for (Long aid : cmdletInfo.getAids()) {
      if (idToActions.containsKey(aid)) {
        ActionInfo toLaunch = idToActions.get(aid);
        args =  new HashMap<>();
        args.putAll(toLaunch.getArgs());
        launchActions.add(
          new LaunchAction(toLaunch.getActionId(), toLaunch.getActionName(), args));
      }
    }
    return new LaunchCmdlet(cmdletInfo.getCid(), launchActions);
  }

  public CmdletInfo getCmdletInfo(long cid) throws IOException {
    if (idToCmdlets.containsKey(cid)) {
      return idToCmdlets.get(cid);
    }
    try {
      return metaStore.getCmdletById(cid);
    } catch (MetaStoreException e) {
      LOG.error("CmdletId -> [ {} ], get CmdletInfo from DB error", cid, e);
      throw new IOException(e);
    }
  }

  public CmdletGroup listCmdletsInfo(long rid, long pageIndex, long numPerPage,
      List<String> orderBy,
      List<Boolean> isDesc) throws IOException, MetaStoreException {
    List<CmdletInfo> cmdlets = metaStore.listPageCmdlets(rid,
      (pageIndex - 1) * numPerPage, numPerPage, orderBy, isDesc);
    return new CmdletGroup(cmdlets, metaStore.getNumCmdletsByRid(rid));
  }

  public List<CmdletInfo> listCmdletsInfo(long rid, CmdletState cmdletState) throws IOException {
    List<CmdletInfo> result = new ArrayList<>();
    try {
      if (rid == -1) {
        result.addAll(metaStore.getCmdlets(null, null, cmdletState));
      } else {
        result.addAll(metaStore.getCmdlets(null, String.format("= %d", rid), cmdletState));
      }
    } catch (MetaStoreException e) {
      LOG.error("RuleId -> [ {} ], List CmdletInfo from DB error", rid, e);
      throw new IOException(e);
    }
    for (CmdletInfo info : idToCmdlets.values()) {
      if (info.getRid() == rid && info.getState().equals(cmdletState)) {
        result.add(info);
      }
    }
    return result;
  }

  public List<CmdletInfo> listCmdletsInfo(long rid) throws IOException {
    Map<Long, CmdletInfo> result = new HashMap<>();
    try {
      String ridCondition = rid == -1 ? null : String.format("= %d", rid);
      for (CmdletInfo info : metaStore.getCmdlets(null, ridCondition, null)) {
        result.put(info.getCid(), info);
      }
    } catch (MetaStoreException e) {
      LOG.error("RuleId -> [ {} ], List CmdletInfo from DB error", rid, e);
      throw new IOException(e);
    }
    for (CmdletInfo info : idToCmdlets.values()) {
      if (info.getRid() == rid) {
        result.put(info.getCid(), info);
      }
    }
    return Lists.newArrayList(result.values());
  }

  public void activateCmdlet(long cid) throws IOException {
    // Currently the default cmdlet status is pending, do nothing here
  }

  public void disableCmdlet(long cid) throws IOException {
    if (idToCmdlets.containsKey(cid)) {
      CmdletInfo info = idToCmdlets.get(cid);
      onCmdletStatusUpdate(
        new CmdletStatus(info.getCid(), System.currentTimeMillis(), CmdletState.DISABLED));

      synchronized (pendingCmdlet) {
        if (pendingCmdlet.contains(cid)) {
          pendingCmdlet.remove(cid);
        }
      }

      if (schedulingCmdlet.contains(cid)) {
        schedulingCmdlet.remove(cid);
      }

      if (scheduledCmdlet.contains(cid)) {
        scheduledCmdlet.remove(cid);
      }

      // Wait status update from status reporter, so need to update to MetaStore
      if (runningCmdlets.contains(cid)) {
        dispatcher.stopCmdlet(cid);
      }
    }
  }

  /**
   * Drop all unfinished cmdlets.
   *
   * @param ruleId
   * @throws IOException
   */
  public void dropRuleCmdlets(long ruleId) throws IOException {
    List<Long> cids = new ArrayList<>();
    for (CmdletInfo info : idToCmdlets.values()) {
      if (info.getRid() == ruleId && !CmdletState.isTerminalState(info.getState())) {
        cids.add(info.getCid());
      }
    }
    batchDeleteCmdlet(cids);
  }

  //Todo: optimize this function.
  private void cmdletFinished(long cmdletId) throws IOException {
    numCmdletsFinished.incrementAndGet();
    CmdletInfo cmdletInfo = idToCmdlets.get(cmdletId);
    if (cmdletInfo == null) {
      LOG.debug("CmdletInfo [id={}] does not exist in idToCmdlets.", cmdletId);
      return;
    }

    dispatcher.onCmdletFinished(cmdletInfo.getCid());
    runningCmdlets.remove(cmdletId);
    idToLaunchCmdlet.remove(cmdletId);

    flushCmdletInfo(cmdletInfo);
  }

  private void cmdletFinishedInternal(CmdletInfo cmdletInfo, boolean success) throws IOException {
    numCmdletsFinished.incrementAndGet();
    ActionInfo actionInfo;
    for (Long aid : cmdletInfo.getAids()) {
      actionInfo = idToActions.get(aid);
      synchronized (actionInfo) {
        // Set all action as finished
        actionInfo.setProgress(1.0F);
        actionInfo.setFinished(true);
        actionInfo.setCreateTime(cmdletInfo.getStateChangedTime());
        actionInfo.setFinishTime(cmdletInfo.getStateChangedTime());
        actionInfo.setExecHost(ActiveServerInfo.getInstance().getId());
        actionInfo.setSuccessful(success);
      }
    }
  }

  public void deleteCmdlet(long cid) throws IOException {
    this.disableCmdlet(cid);
    try {
      metaStore.deleteCmdlet(cid);
      metaStore.deleteCmdletActions(cid);
    } catch (MetaStoreException e) {
      LOG.error("CmdletId -> [ {} ], delete from DB error", cid, e);
      throw new IOException(e);
    }
  }

  private void batchDeleteCmdlet(List<Long> cids) throws IOException {
    for (Long cid: cids) {
      this.disableCmdlet(cid);
    }
    synchronized (tobeDeletedCmd) {
      tobeDeletedCmd.addAll(cids);
    }
  }

  public ActionInfo getActionInfo(long actionID) throws IOException {
    if (idToActions.containsKey(actionID)) {
      return idToActions.get(actionID);
    }
    try {
      return metaStore.getActionById(actionID);
    } catch (MetaStoreException e) {
      LOG.error("ActionId -> [ {} ], get ActionInfo from DB error", actionID, e);
      throw new IOException(e);
    }
  }

  public List<ActionInfo> listNewCreatedActions(String actionName,
      int actionNum) throws IOException {
    try {
      return metaStore.getNewCreatedActions(actionName, actionNum);
    } catch (MetaStoreException e) {
      LOG.error("ActionName -> [ {} ], list ActionInfo from DB error", actionName, e);
      throw new IOException(e);
    }
  }

  public List<ActionInfo> listNewCreatedActions(String actionName,
      int actionNum,
      boolean finished) throws IOException {
    try {
      return metaStore.getNewCreatedActions(actionName, actionNum, finished);
    } catch (MetaStoreException e) {
      LOG.error("ActionName -> [ {} ], get from DB error", actionName, e);
      throw new IOException(e);
    }
  }

  public List<ActionInfo> listNewCreatedActions(String actionName,
      boolean successful,
      int actionNum) throws IOException {
    try {
      return metaStore.getNewCreatedActions(actionName, successful, actionNum);
    } catch (MetaStoreException e) {
      LOG.error("ActionName -> [ {} ], get from DB error", actionName, e);
      throw new IOException(e);
    }
  }


  public List<ActionInfo> listNewCreatedActions(int actionNum) throws IOException {
    try {
      Map<Long, ActionInfo> actionInfos = new HashMap<>();
      for (ActionInfo info : metaStore.getNewCreatedActions(actionNum)) {
        actionInfos.put(info.getActionId(), info);
      }
      actionInfos.putAll(idToActions);
      return Lists.newArrayList(actionInfos.values());
    } catch (MetaStoreException e) {
      LOG.error("Get Finished Actions from DB error", e);
      throw new IOException(e);
    }
  }

  private class DetailedFileActionGroup {
    private List<DetailedFileAction> detailedFileActions;
    private long totalNumOfActions;

    public DetailedFileActionGroup(List<DetailedFileAction> detailedFileActions,
                                   long totalNumOfActions) {
      this.detailedFileActions = detailedFileActions;
      this.totalNumOfActions = totalNumOfActions;
    }
  }

  private class CmdletGroup {
    private List<CmdletInfo> cmdlets;
    private long totalNumOfCmdlets;

    public CmdletGroup(List<CmdletInfo> cmdlets, long totalNumOfCmdlets) {
      this.cmdlets = cmdlets;
      this.totalNumOfCmdlets = totalNumOfCmdlets;
    }
  }

  private class ActionGroup {
    private List<ActionInfo> actions;
    private long totalNumOfActions;

    public ActionGroup() {
      this.totalNumOfActions = 0;
    }

    public ActionGroup(List<ActionInfo> actions, long totalNumOfActions) {
      this.actions = actions;
      this.totalNumOfActions = totalNumOfActions;
    }
  }

  public ActionGroup listActions(long pageIndex, long numPerPage,
      List<String> orderBy, List<Boolean> isDesc) throws IOException, MetaStoreException {
    if (pageIndex == Long.parseLong("0")) {
      if (tmpActions.totalNumOfActions != 0) {
        return tmpActions;
      } else {
        pageIndex = 1;
      }
    }
    List<ActionInfo> infos = metaStore.listPageAction((pageIndex - 1) * numPerPage,
        numPerPage, orderBy, isDesc);
    for (ActionInfo info : infos) {
      ActionInfo memInfo = idToActions.get(info.getActionId());
      if (memInfo != null) {
        info.setCreateTime(memInfo.getCreateTime());
        info.setProgress(memInfo.getProgress());
      }
    }
    tmpActions = new ActionGroup(infos, metaStore.getCountOfAllAction());
    return tmpActions;
  }

  public ActionGroup searchAction(String path, long pageIndex, long numPerPage,
      List<String> orderBy, List<Boolean> isDesc) throws IOException {
    try {
      if (pageIndex == Long.parseLong("0")) {
        if (tmpActions.totalNumOfActions != 0) {
          return tmpActions;
        } else {
          pageIndex = 1;
        }
      }
      long[] total = new long[1];
      List<ActionInfo> infos =  metaStore.searchAction(path, (pageIndex - 1) * numPerPage,
          numPerPage, orderBy, isDesc, total);
      for (ActionInfo info : infos) {
        LOG.debug("[metaStore search] " + info.getActionName());
        ActionInfo memInfo = idToActions.get(info.getActionId());
        if (memInfo != null) {
          info.setCreateTime(memInfo.getCreateTime());
          info.setProgress(memInfo.getProgress());
        }
      }
      tmpActions = new ActionGroup(infos, total[0]);
      return tmpActions;
    } catch (MetaStoreException e) {
      LOG.error("Search [ {} ], Get Finished Actions by search from DB error", path, e);
      throw new IOException(e);
    }
  }

  public List<ActionInfo> getActions(List<Long> aids) throws IOException {
    try {
      return metaStore.getActions(aids);
    } catch (MetaStoreException e) {
      LOG.error("Get Actions by aid list [{}] from DB error", aids.toString());
      throw new IOException(e);
    }
  }

  public List<ActionInfo> getActions(long rid, int size) throws IOException {
    try {
      return metaStore.getActions(rid, size);
    } catch (MetaStoreException e) {
      LOG.error("RuleId -> [ {} ], Get Finished Actions by rid and size from DB error", rid, e);
      throw new IOException(e);
    }
  }

  public DetailedFileActionGroup getFileActions(long rid,
      long pageIndex,
      long numPerPage)
    throws IOException, MetaStoreException {
    List<DetailedFileAction> detailedFileActions = metaStore.listFileActions(rid,
      (pageIndex - 1) * numPerPage, numPerPage);
    return new DetailedFileActionGroup(detailedFileActions, metaStore.getNumFileAction(rid));
  }

  public List<DetailedFileAction> getFileActions(long rid, int size) throws IOException {
    try {
      return metaStore.listFileActions(rid, size);
    } catch (MetaStoreException e) {
      LOG.error("RuleId -> [ {} ], Get Finished Actions by rid and size from DB error", rid, e);
      throw new IOException(e);
    }
  }

  public void updateCmdletExecHost(long cmdletId, String host) throws IOException {
    CmdletInfo cmdlet = getCmdletInfo(cmdletId);
    if (cmdlet == null) {
      return;
    }

    ActionInfo action;
    for (long id : cmdlet.getAids()) {
      action = getActionInfo(id);
      if (action != null) {
        action.setExecHost(host);
      }
    }
  }

  /**
   * Delete all cmdlets related with rid.
   * @param rid
   * @throws IOException
   */
  public void deleteCmdletByRule(long rid) throws IOException {
    List<CmdletInfo> cmdletInfoList = listCmdletsInfo(rid, null);
    if (cmdletInfoList == null || cmdletInfoList.size() == 0) {
      return;
    }
    List<Long> cids = new ArrayList<>();
    for (CmdletInfo cmdletInfo : cmdletInfoList) {
      cids.add(cmdletInfo.getCid());
    }
    batchDeleteCmdlet(cids);
  }

  public void updateStatus(StatusMessage status) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got status update: " + status);
    }
    try {
      if (status instanceof CmdletStatusUpdate) {
        CmdletStatusUpdate statusUpdate = (CmdletStatusUpdate) status;
        onCmdletStatusUpdate(statusUpdate.getCmdletStatus());
      } else if (status instanceof StatusReport) {
        onStatusReport((StatusReport) status);
      }
    } catch (IOException e) {
      LOG.error(String.format("Update status %s failed with %s", status, e));
    } catch (ActionException e) {
      LOG.error("Action Status error {}", e);
    }
  }

  private void onStatusReport(StatusReport report) throws IOException, ActionException {
    List<ActionStatus> actionStatusList = report.getActionStatuses();
    if (actionStatusList == null) {
      return;
    }
    for (ActionStatus actionStatus : actionStatusList) {
      onStatusUpdate(actionStatus);
    }
  }

  private void onStatusUpdate(ActionStatus actionStatus) throws IOException,
      ActionException {
    onActionStatusUpdate(actionStatus);
    ActionInfo actionInfo = idToActions.get(actionStatus.getActionId());
    inferCmdletStatus(actionInfo);
  }

  public void onCmdletStatusUpdate(CmdletStatus status) throws IOException {
    if (status == null) {
      return;
    }
    long cmdletId = status.getCmdletId();
    CmdletInfo cmdletInfo = idToCmdlets.get(cmdletId);
    if (cmdletInfo != null) {
      synchronized (cmdletInfo) {
        if (CmdletState.isTerminalState(cmdletInfo.getState())) {
          return;
        }
        CmdletState state = status.getCurrentState();
        cmdletInfo.setState(state);
        cmdletInfo.setStateChangedTime(status.getStateUpdateTime());
        if (CmdletState.isTerminalState(state)) {
          cmdletFinished(cmdletId);
        } else if (state == CmdletState.DISPATCHED) {
          flushCmdletInfo(cmdletInfo);
        }
      }
    }
  }

  public void onActionStatusUpdate(ActionStatus status)
    throws IOException, ActionException {
    if (status == null) {
      return;
    }
    long actionId = status.getActionId();
    if (idToActions.containsKey(actionId)) {
      ActionInfo actionInfo = idToActions.get(actionId);
      CmdletInfo cmdletInfo = idToCmdlets.get(status.getCmdletId());
      synchronized (actionInfo) {
        if (!actionInfo.isFinished()) {
          actionInfo.setLog(status.getLog());
          actionInfo.setResult(status.getResult());
          if (!status.isFinished()) {
            actionInfo.setProgress(status.getPercentage());
            if (actionInfo.getCreateTime() == 0) {
              actionInfo.setCreateTime(cmdletInfo.getGenerateTime());
            }
            actionInfo.setFinishTime(System.currentTimeMillis());
          } else {
            actionInfo.setProgress(1.0F);
            actionInfo.setFinished(true);
            actionInfo.setCreateTime(status.getStartTime());
            actionInfo.setFinishTime(status.getFinishTime());
            if (status.getThrowable() != null) {
              actionInfo.setSuccessful(false);
            } else {
              actionInfo.setSuccessful(true);
              updateStorageIfNeeded(actionInfo);
            }
            int actionIndex = 0;
            for (long id : cmdletInfo.getAids()) {
              if (id == actionId) {
                break;
              }
              actionIndex++;
            }
            for (ActionScheduler p : schedulers.get(actionInfo.getActionName())) {
              p.onActionFinished(cmdletInfo, actionInfo, actionIndex);
            }
          }
        }
      }
    } else {
      // Updating action info which is not pending or running
    }
  }

  private void inferCmdletStatus(ActionInfo actionInfo) throws IOException, ActionException {
    if (actionInfo == null) {
      return;
    }
    if (!actionInfo.isFinished()) {
      return;
    }
    long actionId = actionInfo.getActionId();
    long cmdletId = actionInfo.getCmdletId();
    CmdletInfo cmdletInfo = idToCmdlets.get(cmdletId);
    List<Long> aids = cmdletInfo.getAids();
    int index = aids.indexOf(actionId);
    if (!actionInfo.isSuccessful()) {
      for (int i = index + 1; i < aids.size(); i++) {
        // Use current action's finish time to set start/finish time for
        // subsequent action(s).
        ActionStatus actionStatus = ActionStatusFactory.createSkipActionStatus(
            cmdletId, i == aids.size() - 1, aids.get(i),
            actionInfo.getFinishTime(), actionInfo.getFinishTime());
        onActionStatusUpdate(actionStatus);
      }
      CmdletStatus cmdletStatus =
        new CmdletStatus(cmdletId, actionInfo.getFinishTime(), CmdletState.FAILED);
      onCmdletStatusUpdate(cmdletStatus);
    } else {
      if (index == aids.size() - 1) {
        CmdletStatus cmdletStatus =
          new CmdletStatus(cmdletId, actionInfo.getFinishTime(), CmdletState.DONE);
        onCmdletStatusUpdate(cmdletStatus);
      }
    }
  }

  private void flushCmdletInfo(CmdletInfo info) throws IOException {
    cacheCmd.put(info.getCid(), info);
  }

  //Todo: remove this implementation
  private void updateStorageIfNeeded(ActionInfo info) throws ActionException {
    SmartAction action = ActionRegistry.createAction(info.getActionName());
    if (action instanceof AbstractMoveFileAction) {
      String policy = ((AbstractMoveFileAction) action).getStoragePolicy();
      Map<String, String> args = info.getArgs();
      if (policy == null) {
        policy = args.get(AbstractMoveFileAction.STORAGE_POLICY);
      }
      String path = args.get(AbstractMoveFileAction.FILE_PATH);
      try {
        String result = info.getResult();
        result = result == null ? "" : result;
        if (!result.contains("UpdateStoragePolicy=false")) {
          metaStore.updateFileStoragePolicy(path, policy);
        }
      } catch (MetaStoreException e) {
        LOG.error("Failed to update storage policy {} for file {}", policy, path, e);
      }
    }
  }

  protected List<ActionInfo> createActionInfos(CmdletDescriptor cmdletDescriptor, long cid)
    throws IOException {
    List<ActionInfo> actionInfos = new ArrayList<>();
    for (int index = 0; index < cmdletDescriptor.getActionSize(); index++) {
      Map<String, String> args = cmdletDescriptor.getActionArgs(index);
      ActionInfo actionInfo =
        new ActionInfo(
          maxActionId.getAndIncrement(),
          cid,
          cmdletDescriptor.getActionName(index),
          args,
          "",
          "",
          false,
          0,
          false,
          0,
          0);
      actionInfos.add(actionInfo);
    }
    return actionInfos;
  }

  private class ScheduleTask implements Runnable {
    public ScheduleTask() {
    }

    @Override
    public void run() {
      try {
        int nScheduled;
        do {
          nScheduled = scheduleCmdlet();
          totalScheduled += nScheduled;
        } while (nScheduled != 0);
      } catch (Throwable t) {
        // no meaningful info, ignore
      }
    }
  }

  private class FlushCachedCmdletsTask implements Runnable {

    @Override
    public void run() {
      try {
        batchSyncCmdAction();
      } catch (Throwable t) {
        LOG.debug("", t);
      }
    }
  }

  private class CmdletPurgeTask implements Runnable {
    private int maxNumRecords;
    private long maxLifeTime;
    private long lastDelTimeStamp = System.currentTimeMillis();
    private long lifeCheckInterval;
    private int succ = 0;

    public CmdletPurgeTask(SmartConf conf) throws IOException {
      maxNumRecords = conf.getInt(SmartConfKeys.SMART_CMDLET_HIST_MAX_NUM_RECORDS_KEY,
        SmartConfKeys.SMART_CMDLET_HIST_MAX_NUM_RECORDS_DEFAULT);
      String lifeString = conf.get(SmartConfKeys.SMART_CMDLET_HIST_MAX_RECORD_LIFETIME_KEY,
        SmartConfKeys.SMART_CMDLET_HIST_MAX_RECORD_LIFETIME_DEFAULT);
      maxLifeTime = StringUtil.pharseTimeString(lifeString);
      if (maxLifeTime == -1) {
        throw new IOException("Invalid value format for configure option. "
          + SmartConfKeys.SMART_CMDLET_HIST_MAX_RECORD_LIFETIME_KEY + "=" + lifeString);
      }
      lifeCheckInterval = maxLifeTime / 20 > 5000 ? (maxLifeTime / 20) : 5000;
    }

    @Override
    public void run() {
      try {
        long ts = System.currentTimeMillis();
        if (ts - lastDelTimeStamp >= lifeCheckInterval) {
          numCmdletsFinished.getAndAdd(
              -metaStore.deleteFinishedCmdletsWithGenTimeBefore(ts - maxLifeTime));
          lastDelTimeStamp = ts;
        }

        long finished = numCmdletsFinished.get();
        if (finished > maxNumRecords * 1.05) {
          numCmdletsFinished.getAndAdd(-metaStore.deleteKeepNewCmdlets(maxNumRecords));
          succ = 0;
        } else if (finished > maxNumRecords) {
          if (succ++ > 5) {
            numCmdletsFinished.getAndAdd(-metaStore.deleteKeepNewCmdlets(maxNumRecords));
            succ = 0;
          }
        }
      } catch (MetaStoreException e) {
        LOG.error("Exception when purging cmdlets.", e);
      }
    }
  }

  private class DetectFailedActionTask implements Runnable {

    public void run() {
      try {
        List<Long> cids = new ArrayList<>();
        cids.addAll(idToLaunchCmdlet.keySet());
        for (Long cid : cids) {
          CmdletInfo cmdletInfo = idToCmdlets.get(cid);
          if (cmdletInfo == null) {
            continue;
          }
          if (cmdletInfo.getState() == CmdletState.DISPATCHED
            || cmdletInfo.getState() == CmdletState.EXECUTING) {
            for (long id : cmdletInfo.getAids()) {
              ActionInfo actionInfo = idToActions.get(id);
              if (!isTimeout(actionInfo)) {
                continue;
              }
              // For timeout action, speculate its status and set result
              // if needed.
              if (isSuccessfulBySpeculation(actionInfo)) {
                ActionStatus actionStatus =
                    ActionStatusFactory.createSuccessActionStatus(
                        cmdletInfo, actionInfo);
                onStatusUpdate(actionStatus);
              } else {
                ActionStatus actionStatus =
                    ActionStatusFactory.createTimeoutActionStatus(
                        cmdletInfo, actionInfo);
                onStatusUpdate(actionStatus);
              }
            }
          }
        }
      } catch (ActionException e) {
        LOG.error(e.getMessage());
      } catch (IOException e) {
        LOG.error(e.getMessage());
      } catch (Throwable t) {
        LOG.error("Unexpected exception occurs.", t);
      }
    }

    public boolean isTimeout(ActionInfo actionInfo) {
      if (actionInfo.isFinished() || actionInfo.getFinishTime() == 0) {
        return false;
      }
      long currentTime = System.currentTimeMillis();
      return currentTime - actionInfo.getFinishTime() > timeout;
    }

    public boolean isSuccessfulBySpeculation(ActionInfo actionInfo) {
      for (ActionScheduler p : schedulers.get(actionInfo.getActionName())) {
        // If it is successful according to one scheduler's speculation,
        // we view it as fact.
        if (p.isSuccessfulBySpeculation(actionInfo)) {
          return true;
        }
      }
      return false;
    }
  }
}
