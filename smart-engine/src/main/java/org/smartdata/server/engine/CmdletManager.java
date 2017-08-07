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
import org.smartdata.actions.ActionException;
import org.smartdata.actions.ActionRegistry;
import org.smartdata.actions.SmartAction;
import org.smartdata.hdfs.action.AbstractMoveFileAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.protocol.message.ActionFinished;
import org.smartdata.protocol.message.ActionStarted;
import org.smartdata.protocol.message.ActionStatus;
import org.smartdata.protocol.message.ActionStatusReport;
import org.smartdata.protocol.message.CmdletStatusUpdate;
import org.smartdata.protocol.message.StatusMessage;
import org.smartdata.server.engine.cmdlet.ActionSchedulerServiceFactory;
import org.smartdata.server.engine.cmdlet.CmdletDispatcher;
import org.smartdata.server.engine.cmdlet.CmdletExecutorService;
import org.smartdata.metastore.ActionPreProcessService;
import org.smartdata.model.actions.ActionPreProcessor;
import org.smartdata.model.LaunchAction;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
 * The map idToCmdlets stores all the recent CmdletInfos, including pending and running Cmdlets.
 * After the Cmdlet is finished or cancelled or failed, it's status will be flush to DB.
 */
public class CmdletManager extends AbstractService {
  private final Logger LOG = LoggerFactory.getLogger(CmdletManager.class);
  private ScheduledExecutorService executorService;
  private CmdletDispatcher dispatcher;
  private MetaStore metaStore;
  private AtomicLong maxActionId;
  private AtomicLong maxCmdletId;

  private Queue<CmdletInfo> pendingCmdlet;
  private List<Long> runningCmdlets;
  private Map<Long, CmdletInfo> idToCmdlets;
  private Map<Long, ActionInfo> idToActions;
  private Map<String, Long> fileLocks;
  private ListMultimap<String, ActionPreProcessor> preExecuteProcessor = ArrayListMultimap.create();
  private List<ActionPreProcessService> preProcessServices = new ArrayList<>();

  public CmdletManager(ServerContext context) {
    super(context);

    this.metaStore = context.getMetaStore();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.dispatcher = new CmdletDispatcher(context, this);
    this.runningCmdlets = new ArrayList<>();
    this.pendingCmdlet = new LinkedBlockingQueue<>();
    this.idToCmdlets = new ConcurrentHashMap<>();
    this.idToActions = new ConcurrentHashMap<>();
    this.fileLocks = new ConcurrentHashMap<>();
  }

  @VisibleForTesting
  void setDispatcher(CmdletDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void init() throws IOException {
    try {
      maxActionId = new AtomicLong(metaStore.getMaxActionId());
      maxCmdletId = new AtomicLong(metaStore.getMaxCmdletId());

      preProcessServices = ActionSchedulerServiceFactory.createServices(
          getContext().getConf(), getContext(), metaStore, false);

      for (ActionPreProcessService s : preProcessServices) {
        s.init();
        List<String> actions = s.getSupportedActions();
        for (String a : actions) {
          preExecuteProcessor.put(a, s);
        }
      }
    } catch (Exception e) {
      LOG.error("DB Connection error! Get Max CommandId/ActionId fail!", e);
      throw new IOException(e);
    }
  }

  @Override
  public void start() throws IOException {
    executorService.scheduleAtFixedRate(
        new ScheduleTask(this.dispatcher), 1000, 1000, TimeUnit.MILLISECONDS);
    for (ActionPreProcessService s : preProcessServices) {
      s.start();
    }
  }

  @Override
  public void stop() throws IOException {
    for (int i = preProcessServices.size() - 1; i >=0 ; i--) {
      preProcessServices.get(i).stop();
    }
    executorService.shutdown();
    dispatcher.shutDownExcutorServices();
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
      LOG.error("Cmdlet format is not correct", e);
      throw new IOException(e);
    }
  }

  public long submitCmdlet(CmdletDescriptor cmdletDescriptor) throws IOException {
    LOG.debug(String.format("Received Cmdlet -> [ %s ]", cmdletDescriptor.getCmdletString()));
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
    for (ActionInfo actionInfo : actionInfos) {
      cmdletInfo.addAction(actionInfo.getActionId());
    }
    for (int index = 0; index < cmdletDescriptor.actionSize(); index++) {
      if (!ActionRegistry.registeredAction(cmdletDescriptor.getActionName(index))) {
        throw new IOException(
          String.format("Submit Cmdlet %s error! Action names are not correct!", cmdletInfo));
      }
    }

    Set<String> filesLocked = lockMovefileActionFiles(actionInfos);

    try {
      metaStore.insertCmdletTable(cmdletInfo);
      metaStore.insertActionsTable(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
    } catch (MetaStoreException e) {
      LOG.error("Submit Command {} to DB error!", cmdletInfo);
      try {
        for (String file : filesLocked) {
          fileLocks.remove(file);
        }
        metaStore.deleteCmdlet(cmdletInfo.getCid());
      } catch (MetaStoreException e1) {
        LOG.error("Delete Command {} from DB error!", cmdletInfo, e);
      }
      throw new IOException(e);
    }
    pendingCmdlet.add(cmdletInfo);
    idToCmdlets.put(cmdletInfo.getCid(), cmdletInfo);
    for (ActionInfo actionInfo : actionInfos) {
      idToActions.put(actionInfo.getActionId(), actionInfo);
    }
    return cmdletInfo.getCid();
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

  public LaunchCmdlet getNextCmdletToRun() throws IOException {
    CmdletInfo cmdletInfo = pendingCmdlet.poll();
    if (cmdletInfo == null) {
      return null;
    }
    List<LaunchAction> launchActions = new ArrayList<>();
    for (Long aid : cmdletInfo.getAids()) {
      if (idToActions.containsKey(aid)) {
        ActionInfo toLaunch = idToActions.get(aid);
        launchActions.add(
            new LaunchAction(toLaunch.getActionId(), toLaunch.getActionName(), toLaunch.getArgs()));
      }
    }
    runningCmdlets.add(cmdletInfo.getCid());
    return new LaunchCmdlet(cmdletInfo.getCid(), launchActions);
  }

  public CmdletInfo getCmdletInfo(long cid) throws IOException {
    if (idToCmdlets.containsKey(cid)) {
      return idToCmdlets.get(cid);
    }
    try {
      return metaStore.getCmdletById(cid);
    } catch (MetaStoreException e) {
      LOG.error("Get CmdletInfo with ID {} from DB error! {}", cid, e);
      throw new IOException(e);
    }
  }

  public List<CmdletInfo> listCmdletsInfo(long rid, CmdletState cmdletState) throws IOException {
    List<CmdletInfo> result = new ArrayList<>();
    try {
      if (rid == -1) {
        result.addAll(metaStore.getCmdletsTableItem(null, null, cmdletState));
      } else {
        result.addAll(metaStore.getCmdletsTableItem(null, String.format("= %d", rid), cmdletState));
      }
    } catch (MetaStoreException e) {
      LOG.error("List CmdletInfo from DB error! Conditions rid {}", rid, e);
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
      for (CmdletInfo info : metaStore.getCmdletsTableItem(null, ridCondition, null)) {
        result.put(info.getCid(), info);
      }
    } catch (MetaStoreException e) {
      LOG.error("List CmdletInfo from DB error! Conditions rid {}", rid, e);
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
      if (pendingCmdlet.contains(info)) {
        pendingCmdlet.remove(info);
        info.setState(CmdletState.DISABLED);
        this.cmdletFinished(cid);
      }
      // Wait status update from status reporter, so need to update to MetaStore
      if (runningCmdlets.contains(cid)) {
        dispatcher.stop(cid);
      }
    }
  }

  //Todo: optimize this function.
  private void cmdletFinished(long cmdletId) throws IOException {
    CmdletInfo cmdletInfo = idToCmdlets.remove(cmdletId);
    if (cmdletInfo != null) {
      flushCmdletInfo(cmdletInfo);
    }
    runningCmdlets.remove(cmdletId);

    List<ActionInfo> removed = new ArrayList<>();
    for (Iterator<Map.Entry<Long, ActionInfo>> it = idToActions.entrySet().iterator(); it.hasNext();) {
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
    } catch (MetaStoreException e) {
      LOG.error("Delete Cmdlet {} from DB error!", cid, e);
      throw new IOException(e);
    }
  }

  public int getCmdletsSizeInCache() {
    return idToCmdlets.size();
  }

  public ActionInfo getActionInfo(long actionID) throws IOException {
    if (idToActions.containsKey(actionID)) {
      return idToActions.get(actionID);
    }
    try {
      return metaStore.getActionById(actionID);
    } catch (MetaStoreException e) {
      LOG.error("Get ActionInfo of {} from DB error!", actionID, e);
      throw new IOException(e);
    }
  }

  public List<ActionInfo> listNewCreatedActions(int actionNum) throws IOException {
    try {
      Map<Long, ActionInfo> actionInfos = new HashMap<>();
      for (ActionInfo info : metaStore.getNewCreatedActionsTableItem(actionNum)) {
        actionInfos.put(info.getActionId(), info);
      }
      actionInfos.putAll(idToActions);
      return Lists.newArrayList(actionInfos.values());
    } catch (MetaStoreException e) {
      LOG.error("Get Finished Actions from DB error", e);
      throw new IOException(e);
    }
  }

  /**
   * Delete all cmdlets related with rid
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
        //Todo: recover cmdlet?
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
        actionInfo.setProgress(status.getPercentage());
        actionInfo.setLog(status.getLog());
        actionInfo.setResult(status.getResult());
        actionInfo.setFinishTime(System.currentTimeMillis());
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
      actionInfo.setFinished(true);
      actionInfo.setFinishTime(finished.getTimestamp());
      actionInfo.setResult(finished.getResult());
      actionInfo.setLog(finished.getLog());
      actionInfo.setProgress(1.0F);
      unLockFileIfNeeded(actionInfo);
      if (finished.getThrowable() != null) {
        actionInfo.setSuccessful(false);
      } else {
        actionInfo.setSuccessful(true);
        updateStorageIfNeeded(actionInfo);
      }
    } else {
      // Updating action status which is not pending or running
    }
  }

  private void flushCmdletInfo(CmdletInfo info) throws IOException {
    try {
      metaStore.updateCmdletStatus(info.getCid(), info.getRid(), info.getState());
    } catch (MetaStoreException e) {
      LOG.error("Batch Cmdlet Status Update error!", e);
      throw new IOException(e);
    }
  }

  private void flushActionInfos(List<ActionInfo> infos) throws IOException {
    try {
      metaStore.updateActionsTable(infos.toArray(new ActionInfo[infos.size()]));
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

  protected List<ActionInfo> createActionInfos(CmdletDescriptor cmdletDescriptor, long cid) throws IOException {
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
    private final CmdletDispatcher dispatcher;

    public ScheduleTask(CmdletDispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    public void run() {
      while (dispatcher.canDispatchMore()) {
        try {
          LaunchCmdlet launchCmdlet = getNextCmdletToRun();
          if (launchCmdlet == null) {
            break;
          } else {
            cmdletPreExecutionProcess(launchCmdlet);
            dispatcher.dispatch(launchCmdlet);
          }
        } catch (IOException e) {
          LOG.error("Cmdlet dispatcher error", e);
        }
      }
    }
  }

  public void cmdletPreExecutionProcess(LaunchCmdlet cmdlet) {
    for (LaunchAction action : cmdlet.getLaunchActions()) {
      for (ActionPreProcessor p : preExecuteProcessor.get(action.getActionType())) {
        p.beforeExecution(action);
      }
    }
  }
}
