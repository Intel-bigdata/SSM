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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.action.ActionException;
import org.smartdata.action.ActionRegistry;
import org.smartdata.action.SmartAction;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
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
import org.smartdata.model.action.ActionScheduler;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.ActionFinished;
import org.smartdata.protocol.message.ActionStarted;
import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.ActionStatusReport;
import org.smartdata.protocol.message.CmdletStatusUpdate;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.server.engine.cmdlet.CmdletDispatcher;
import org.smartdata.server.engine.cmdlet.CmdletExecutorService;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.utils.StringUtil;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
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
  private ScheduledExecutorService executorService;
  private CmdletDispatcher dispatcher;
  private MetaStore metaStore;
  private AtomicLong maxActionId;
  private AtomicLong maxCmdletId;

  private int maxNumPendingCmdlets;
  private List<Long> pendingCmdlet;
  private List<Long> schedulingCmdlet;
  private Queue<Long> scheduledCmdlet;
  private Map<Long, LaunchCmdlet> idToLaunchCmdlet;
  private List<Long> runningCmdlets;
  private Map<Long, CmdletInfo> idToCmdlets;
  private Map<Long, ActionInfo> idToActions;
  private Map<String, Long> fileLocks;
  private ListMultimap<String, ActionScheduler> schedulers = ArrayListMultimap.create();
  private List<ActionSchedulerService> schedulerServices = new ArrayList<>();

  private AtomicLong numCmdletsGen = new AtomicLong(0);
  private AtomicLong numCmdletsFinished = new AtomicLong(0);

  private long totalScheduled = 0;
  private CmdletPurgeTask purgeTask;

  public CmdletManager(ServerContext context) throws IOException {
    super(context);

    this.metaStore = context.getMetaStore();
    this.executorService = Executors.newScheduledThreadPool(2);
    this.runningCmdlets = new ArrayList<>();
    this.pendingCmdlet = new LinkedList<>();
    this.schedulingCmdlet = new LinkedList<>();
    this.scheduledCmdlet = new LinkedBlockingQueue<>();
    this.idToLaunchCmdlet = new HashMap<>();
    this.idToCmdlets = new ConcurrentHashMap<>();
    this.idToActions = new ConcurrentHashMap<>();
    this.fileLocks = new ConcurrentHashMap<>();
    this.purgeTask = new CmdletPurgeTask(context.getConf());
    this.dispatcher = new CmdletDispatcher(context, this, scheduledCmdlet,
        idToLaunchCmdlet, runningCmdlets, schedulers);
    maxNumPendingCmdlets = context.getConf().getInt(SmartConfKeys.SMART_CMDLET_MAX_NUM_PENDING_KEY,
        SmartConfKeys.SMART_CMDLET_MAX_NUM_PENDING_DEFAULT);
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
          getContext().getConf(), getContext(), metaStore, false);

      for (ActionSchedulerService s : schedulerServices) {
        s.init();
        List<String> actions = s.getSupportedActions();
        for (String a : actions) {
          schedulers.put(a, s);
        }
      }
      // reload pending cmdlets from metastore
      reloadPendingCmdlets();
      LOG.info("Initialized.");
    } catch (MetaStoreException e) {
      LOG.error("DB Connection error! Get Max CommandId/ActionId fail!", e);
      throw new IOException(e);
    } catch (IOException e) {
      throw e;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  private void reloadPendingCmdlets() throws IOException {
    LOG.info("Reloading Pending Cmdlets from Database.");
    List<CmdletInfo> cmdletInfos = null;
    try {
      cmdletInfos = metaStore.getCmdlets(CmdletState.PENDING);
    } catch (MetaStoreException e) {
      LOG.error("Get pending cmdlets from database error!");
      return;
    }
    if (cmdletInfos == null || cmdletInfos.size() == 0) {
      return;
    }
    for (CmdletInfo cmdletInfo : cmdletInfos) {
      LOG.debug(
          String.format("Reload Pending Cmdlet -> [ %s ]", cmdletInfo.getParameters()));
      List<ActionInfo> actionInfos = null;
      if (cmdletInfo.getAids().size() != 0) {
        for (long aid : cmdletInfo.getAids()) {
          LOG.debug("ActionId -> [ {} ] marked as Failed", aid);
          try {
            metaStore.markActionFailed(aid);
          } catch (MetaStoreException e) {
            LOG.debug("ActionId -> [ {} ] cannot marked as Failed", aid, e);
          }
        }
      }
      cmdletInfo.setState(CmdletState.FAILED);
      try {
        metaStore.updateCmdlet(cmdletInfo);
      } catch (MetaStoreException e1) {
        LOG.error("{} marked as failed error",
            cmdletInfo, e1);
      }
      // LOG.debug(String.format("Received Cmdlet -> [ %s ]",
      //     cmdletDescriptor.getCmdletString()));
      // Check action names
      // checkActionNames(cmdletDescriptor);
      // Let Scheduler check actioninfo onsubmit and add them to cmdletinfo
      // checkActionsOnSubmit(cmdletInfo, actionInfos);
      // Sync cmdletinfo and actionInfos with metastore and cache, add locks if necessary
      // syncCmdAction(cmdletInfo, actionInfos, actionsInDB);
    }
  }

  /**
   * Check if action names in cmdletDescriptor are correct.
   * @param cmdletDescriptor
   * @throws IOException
   */
  private void checkActionNames(
      CmdletDescriptor cmdletDescriptor) throws IOException {
    for (int index = 0; index < cmdletDescriptor.actionSize(); index++) {
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
   * @param cmdletInfo
   * @param actionInfos
   * @throws IOException
   */
  private void checkActionsOnSubmit(CmdletInfo cmdletInfo,
      List<ActionInfo> actionInfos) throws IOException {
    for (ActionInfo actionInfo : actionInfos) {
      for (ActionScheduler p : schedulers.get(actionInfo.getActionName())) {
        if (!p.onSubmit(actionInfo)) {
          throw new IOException(
              String.format("Action rejected by scheduler", actionInfo));
        }
      }
      cmdletInfo.addAction(actionInfo.getActionId());
    }
  }

  /**
   * Sync cmdletinfo and actionInfos with metastore and cache, add locks if necessary.
   * @param cmdletInfo
   * @param actionInfos
   * @param actionsInDB
   * @throws IOException
   */
  private void syncCmdAction(CmdletInfo cmdletInfo,
      List<ActionInfo> actionInfos, boolean actionsInDB) throws IOException {
    Set<String> filesLocked = lockMovefileActionFiles(actionInfos);
    try {
      metaStore.insertCmdlet(cmdletInfo);
      if (!actionsInDB) {
        metaStore.insertActions(
            actionInfos.toArray(new ActionInfo[actionInfos.size()]));
      }
    } catch (MetaStoreException e) {
      LOG.error("{} Sync with DB error", cmdletInfo, e);
      try {
        for (String file : filesLocked) {
          fileLocks.remove(file);
        }
        cmdletInfo.setState(CmdletState.FAILED);
        metaStore.updateCmdlet(cmdletInfo);
      } catch (MetaStoreException e1) {
        LOG.error("{} marked as failed error!", cmdletInfo, e);
      }
      throw new IOException(e);
    }

    for (ActionInfo actionInfo : actionInfos) {
      if (actionInfo.isFinished()) {
        continue;
      }
      idToActions.put(actionInfo.getActionId(), actionInfo);
    }
    idToCmdlets.put(cmdletInfo.getCid(), cmdletInfo);
    synchronized (pendingCmdlet) {
      pendingCmdlet.add(cmdletInfo.getCid());
    }
  }

  @Override
  public void start() throws IOException {
    LOG.info("Starting ...");
    executorService.scheduleAtFixedRate(new CmdletPurgeTask(getContext().getConf()),
        10, 5000, TimeUnit.MILLISECONDS);
    executorService.scheduleAtFixedRate(new ScheduleTask(), 100, 50, TimeUnit.MILLISECONDS);

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
    dispatcher.shutDownExcutorServices();
    LOG.info("Stopped.");
  }

  public void registerExecutorService(CmdletExecutorService executorService) {
    dispatcher.registerExecutorService(executorService);
  }

  public long submitCmdlet(String cmdlet) throws IOException {
    LOG.debug(String.format("Received Cmdlet -> [ %s ]", cmdlet));
    try {
      CmdletDescriptor cmdletDescriptor = CmdletDescriptor.fromCmdletString(cmdlet);
      return submitCmdlet(cmdletDescriptor);
    } catch (ParseException e) {
      LOG.error("Cmdlet -> [ {} ], format is not correct", cmdlet, e);
      throw new IOException(e);
    }
  }

  public long submitCmdlet(CmdletDescriptor cmdletDescriptor) throws IOException {
    LOG.debug(String.format("Received Cmdlet -> [ %s ]", cmdletDescriptor.getCmdletString()));
    if (maxNumPendingCmdlets <= pendingCmdlet.size() + schedulingCmdlet.size()) {
      throw new IOException("Pending cmdlets exceeds value specified by key '"
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
        submitTime);
    List<ActionInfo> actionInfos = createActionInfos(cmdletDescriptor, cmdletInfo.getCid());
    // Check action names
    checkActionNames(cmdletDescriptor);
    // Let Scheduler check actioninfo onsubmit and add them to cmdletinfo
    checkActionsOnSubmit(cmdletInfo, actionInfos);
    // Insert cmdletinfo and actionInfos to metastore and cache, add locks if necessary
    syncCmdAction(cmdletInfo, actionInfos);
    return cmdletInfo.getCid();
  }

  /**
   * Insert cmdletinfo and actions to metastore and cache, add locks if necessary.
   *
   * @param cmdletInfo
   * @param actionInfos
   * @throws IOException
   */
  private void syncCmdAction(CmdletInfo cmdletInfo,
      List<ActionInfo> actionInfos) throws IOException {
    Set<String> filesLocked = lockMovefileActionFiles(actionInfos);
    try {
      metaStore.insertCmdlet(cmdletInfo);
      metaStore.insertActions(
          actionInfos.toArray(new ActionInfo[actionInfos.size()]));
      numCmdletsGen.incrementAndGet();
    } catch (MetaStoreException e) {
      LOG.error("{} submit to DB error", cmdletInfo, e);

      try {
        for (String file : filesLocked) {
          fileLocks.remove(file);
        }
        metaStore.deleteCmdlet(cmdletInfo.getCid());
      } catch (MetaStoreException e1) {
        LOG.error("{} delete from DB error", cmdletInfo, e);
      }
      throw new IOException(e);
    }

    for (ActionInfo actionInfo : actionInfos) {
      idToActions.put(actionInfo.getActionId(), actionInfo);
    }
    idToCmdlets.put(cmdletInfo.getCid(), cmdletInfo);
    synchronized (pendingCmdlet) {
      pendingCmdlet.add(cmdletInfo.getCid());
    }
  }

  private synchronized Set<String> lockMovefileActionFiles(List<ActionInfo> actionInfos)
      throws IOException {
    Map<String, Long> filesToLock = new HashMap<>();
    for (ActionInfo info : actionInfos) {
      SmartAction action;
      try {
        action = ActionRegistry.createAction(info.getActionName());
      } catch (ActionException e) {
        throw new IOException("Failed to create '" + info.getActionName()
            + "' action instance", e);
      }
      if (action instanceof AbstractMoveFileAction) {
        Map<String, String> args = info.getArgs();
        if (args != null && args.size() > 0) {
          String file = args.get(CmdletDescriptor.HDFS_FILE_PATH);
          if (file != null) {
            if (fileLocks.containsKey(file)) {
              LOG.debug("Warning: Other actions are processing {}!", file);
              throw new IOException("Has conflict actions, submit cmdlet failed.");
            } else {
              filesToLock.put(file, info.getActionId());
            }
          }
        }
      }
    }

    fileLocks.putAll(filesToLock);
    return filesToLock.keySet();
  }

  private boolean shouldStopSchedule() {
    int left = dispatcher.getTotalSlotsLeft();
    int total = dispatcher.getTotalSlots();
    if (scheduledCmdlet.size() >= left + total * 0.2) {
      return true;
    }
    return false;
  }

  public int scheduleCmdlet() throws IOException {
    int nScheduled = 0;

    synchronized (pendingCmdlet) {
      if (pendingCmdlet.size() > 0) {
        schedulingCmdlet.addAll(pendingCmdlet);
        pendingCmdlet.clear();
      }
    }

    Iterator<Long> it = schedulingCmdlet.iterator();
    while (it.hasNext() && !shouldStopSchedule()) {
      long id = it.next();
      CmdletInfo cmdlet = idToCmdlets.get(id);
      synchronized (cmdlet) {
        switch (cmdlet.getState()) {
          case CANCELLED:
          case DISABLED:
            it.remove();
            break;

          case PENDING:
            LaunchCmdlet launchCmdlet = createLaunchCmdlet(cmdlet);
            ScheduleResult result = scheduleCmdletActions(cmdlet, launchCmdlet);
            if (result != ScheduleResult.RETRY) {
              it.remove();
            }
            if (result == ScheduleResult.SUCCESS) {
              idToLaunchCmdlet.put(cmdlet.getCid(), launchCmdlet);
              cmdlet.setState(CmdletState.SCHEDULED);
              scheduledCmdlet.add(id);
              nScheduled++;
            } else if (result == ScheduleResult.FAIL) {
              cmdlet.updateState(CmdletState.CANCELLED);
              CmdletStatusUpdate msg = new CmdletStatusUpdate(cmdlet.getCid(),
                  cmdlet.getStateChangedTime(), cmdlet.getState());
              // Mark all actions as finished and successful
              cmdletFinishedInternal(cmdlet);
              onCmdletStatusUpdate(msg);
            }
            break;
        }
      }
    }
    return nScheduled;
  }

  private ScheduleResult scheduleCmdletActions(CmdletInfo info, LaunchCmdlet launchCmdlet) {
    List<Long> actIds = info.getAids();
    int idx = 0;
    int schIdx = 0;
    ActionInfo actionInfo;
    LaunchAction launchAction;
    List<ActionScheduler> actSchedulers;
    ScheduleResult scheduleResult = ScheduleResult.SUCCESS;
    for (idx = 0; idx < actIds.size(); idx++) {
      actionInfo = idToActions.get(actIds.get(idx));
      launchAction = launchCmdlet.getLaunchActions().get(idx);
      actSchedulers = schedulers.get(actionInfo.getActionName());
      if (actSchedulers == null || actSchedulers.size() == 0) {
        continue;
      }

      for (schIdx = 0; schIdx < actSchedulers.size(); schIdx++) {
        ActionScheduler s = actSchedulers.get(schIdx);
        scheduleResult = s.onSchedule(actionInfo, launchAction);
        if (scheduleResult != ScheduleResult.SUCCESS) {
          break;
        }
      }

      if (scheduleResult != ScheduleResult.SUCCESS) {
        break;
      }
    }

    if (scheduleResult == ScheduleResult.SUCCESS) {
      idx--;
      schIdx--;
    }
    postscheduleCmdletActions(actIds, scheduleResult, idx, schIdx);
    return scheduleResult;
  }

  private void postscheduleCmdletActions(List<Long> actions, ScheduleResult result,
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
        actSchedulers.get(sidx).postSchedule(info, result);
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
      List<String> orderBy, List<Boolean> isDesc) throws IOException, MetaStoreException {
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
      synchronized (info) {
        info.updateState(CmdletState.DISABLED);
      }
      synchronized (pendingCmdlet) {
        if (pendingCmdlet.contains(cid)) {
          pendingCmdlet.remove(cid);
          this.cmdletFinished(cid);
        }
      }

      if (scheduledCmdlet.contains(cid)) {
        scheduledCmdlet.remove(cid);
      }

      // Wait status update from status reporter, so need to update to MetaStore
      if (runningCmdlets.contains(cid)) {
        dispatcher.stop(cid);
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
    for (CmdletInfo info : idToCmdlets.values()) {
      if (info.getRid() == ruleId && !CmdletState.isTerminalState(info.getState())) {
        deleteCmdlet(info.getCid());
      }
    }
  }

  //Todo: optimize this function.
  private void cmdletFinished(long cmdletId) throws IOException {
    numCmdletsFinished.incrementAndGet();
    CmdletInfo cmdletInfo = idToCmdlets.remove(cmdletId);
    if (cmdletInfo != null) {
      flushCmdletInfo(cmdletInfo);
    }
    dispatcher.onCmdletFinished(cmdletInfo.getCid());
    runningCmdlets.remove(cmdletId);

    List<ActionInfo> removed = new ArrayList<>();
    for (Iterator<Map.Entry<Long, ActionInfo>> it = idToActions.entrySet().iterator();
        it.hasNext(); ) {
      Map.Entry<Long, ActionInfo> entry = it.next();
      if (entry.getValue().getCmdletId() == cmdletId) {
        it.remove();
        removed.add(entry.getValue());
      }
    }
    for (ActionInfo actionInfo : removed) {
      unLockFileIfNeeded(actionInfo);
    }
    flushActionInfos(removed);
    idToLaunchCmdlet.remove(cmdletId);
  }

  private void cmdletFinishedInternal(CmdletInfo cmdletInfo) throws IOException {
    numCmdletsFinished.incrementAndGet();
    List<ActionInfo> removed = new ArrayList<>();
    ActionInfo actionInfo;
    for (Long aid : cmdletInfo.getAids()) {
      actionInfo = idToActions.get(aid);
      // Set all action as finished
      actionInfo.setProgress(1.0F);
      actionInfo.setFinished(true);
      actionInfo.setFinishTime(System.currentTimeMillis());
      unLockFileIfNeeded(actionInfo);
      idToActions.remove(aid);
    }
    flushActionInfos(removed);
  }

  private void unLockFileIfNeeded(ActionInfo actionInfo) {
    SmartAction action;
    try {
      action = ActionRegistry.createAction(actionInfo.getActionName());
    } catch (ActionException e) {
      return;
    }
    if (action instanceof AbstractMoveFileAction) {
      Map<String, String> args = actionInfo.getArgs();
      if (args != null && args.size() > 0) {
        String file = args.get(CmdletDescriptor.HDFS_FILE_PATH);
        if (file != null && fileLocks.containsKey(file)) {
          fileLocks.remove(file);
        }
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

  public int getCmdletsSizeInCache() {
    return idToCmdlets.size();
  }

  public int getActionsSizeInCache() {
    return idToActions.size();
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
      int actionNum, boolean finished) throws IOException {
    try {
      return metaStore.getNewCreatedActions(actionName, actionNum, finished);
    } catch (MetaStoreException e) {
      LOG.error("ActionName -> [ {} ], get from DB error", actionName, e);
      throw new IOException(e);
    }
  }

  public List<ActionInfo> listNewCreatedActions(String actionName,
      boolean successful, int actionNum) throws IOException {
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

    public ActionGroup(List<ActionInfo> actions, long totalNumOfActions) {
      this.actions = actions;
      this.totalNumOfActions = totalNumOfActions;
    }
  }

  public ActionGroup listActions(long pageIndex, long numPerPage,
      List<String> orderBy, List<Boolean> isDesc) throws IOException, MetaStoreException {
    List<ActionInfo> infos = metaStore.listPageAction((pageIndex - 1) * numPerPage,
        numPerPage, orderBy, isDesc);
    for (ActionInfo info : infos) {
      ActionInfo memInfo = idToActions.get(info.getActionId());
      if (memInfo != null) {
        info.setCreateTime(memInfo.getCreateTime());
        info.setProgress(memInfo.getProgress());
      }
    }

    return new ActionGroup(infos, metaStore.getCountOfAllAction());
  }

  public List<ActionInfo> getActions(long rid, int size) throws IOException {
    try {
      return metaStore.getActions(rid, size);
    } catch (MetaStoreException e) {
      LOG.error("RuleId -> [ {} ], Get Finished Actions by rid and size from DB error", rid, e);
      throw new IOException(e);
    }
  }

  public DetailedFileActionGroup getFileActions(long rid, long pageIndex,
      long numPerPage) throws IOException, MetaStoreException {
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
    for (CmdletInfo cmdletInfo : cmdletInfoList) {
      deleteCmdlet(cmdletInfo.getCid());
    }
  }

  public synchronized void updateStatus(StatusMessage status) {
    LOG.debug("Got status update: " + status);
    try {
      if (status instanceof CmdletStatusUpdate) {
        onCmdletStatusUpdate((CmdletStatusUpdate) status);
      } else if (status instanceof ActionStatusReport) {
        onActionStatusReport((ActionStatusReport) status);
      } else if (status instanceof ActionStarted) {
        onActionStarted((ActionStarted) status);
      } else if (status instanceof ActionFinished) {
        onActionFinished((ActionFinished) status);
      }
    } catch (IOException e) {
      LOG.error(String.format("Update status %s failed with %s", status, e));
    } catch (ActionException e) {
      LOG.error("Action Status error {}", e);
    }
  }

  private void onCmdletStatusUpdate(CmdletStatusUpdate statusUpdate) throws IOException {
    long cmdletId = statusUpdate.getCmdletId();
    if (idToCmdlets.containsKey(cmdletId)) {
      CmdletState state = statusUpdate.getCurrentState();
      CmdletInfo cmdletInfo = idToCmdlets.get(cmdletId);
      cmdletInfo.setState(state);
      //The cmdlet is already finished or terminated, remove status from memory.
      if (CmdletState.isTerminalState(state)) {
        cmdletFinished(cmdletId);
      }
    } else {
      // Updating cmdlet status which is not pending or running
    }
  }

  private void onActionStatusReport(ActionStatusReport report) throws IOException {
    for (ActionStatus status : report.getActionStatuses()) {
      long actionId = status.getActionId();
      if (idToActions.containsKey(actionId)) {
        ActionInfo actionInfo = idToActions.get(actionId);
        synchronized (actionInfo) {
          if (!actionInfo.isFinished()) {
            actionInfo.setProgress(status.getPercentage());
            actionInfo.setLog(status.getLog());
            actionInfo.setResult(status.getResult());
            actionInfo.setFinishTime(System.currentTimeMillis());
          }
        }
      } else {
        // Updating action info which is not pending or running
      }
    }
  }

  private void onActionStarted(ActionStarted started) {
    if (idToActions.containsKey(started.getActionId())) {
      idToActions.get(started.getActionId()).setCreateTime(started.getTimestamp());
    } else {
      // Updating action status which is not pending or running
    }
  }

  private void onActionFinished(ActionFinished finished) throws IOException, ActionException {
    if (idToActions.containsKey(finished.getActionId())) {
      ActionInfo actionInfo = idToActions.get(finished.getActionId());
      synchronized (actionInfo) {
        actionInfo.setProgress(1.0F);
        actionInfo.setFinished(true);
        actionInfo.setFinishTime(finished.getTimestamp());
        actionInfo.setResult(finished.getResult());
        actionInfo.setLog(finished.getLog());
      }
      unLockFileIfNeeded(actionInfo);
      if (finished.getThrowable() != null) {
        actionInfo.setSuccessful(false);
      } else {
        actionInfo.setSuccessful(true);
        updateStorageIfNeeded(actionInfo);
      }
      for (ActionScheduler p : schedulers.get(actionInfo.getActionName())) {
        p.onActionFinished(actionInfo);
      }

    } else {
      // Updating action status which is not pending or running
    }
  }

  private void flushCmdletInfo(CmdletInfo info) throws IOException {
    try {
      metaStore.updateCmdlet(info.getCid(), info.getRid(), info.getState());
    } catch (MetaStoreException e) {
      LOG.error(
          "CmdletId -> [ {} ], CmdletInfo -> [ {} ]. Batch Cmdlet Status Update error!",
          info.getCid(),
          info,
          e);
      throw new IOException(e);
    }
  }

  private void flushActionInfos(List<ActionInfo> infos) throws IOException {
    try {
      metaStore.updateActions(infos.toArray(new ActionInfo[infos.size()]));
    } catch (MetaStoreException e) {
      LOG.error("Write CacheObject to DB error!", e);
      throw new IOException(e);
    }
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
        metaStore.updateFileStoragePolicy(path, policy);
      } catch (MetaStoreException e) {
        LOG.error("Failed to update storage policy {} for file {}", policy, path, e);
      }
    }
  }

  protected List<ActionInfo> createActionInfos(CmdletDescriptor cmdletDescriptor, long cid)
      throws IOException {
    List<ActionInfo> actionInfos = new ArrayList<>();
    for (int index = 0; index < cmdletDescriptor.actionSize(); index++) {
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
      } catch (IOException e) {
        LOG.error("Exception when Scheduling Cmdlet. "
            + scheduledCmdlet.size() + " cmdlets are pending for dispatch.", e);
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
              metaStore.deleteFinishedCmdletsWithGenTimeBefore(ts - maxLifeTime));
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
}
