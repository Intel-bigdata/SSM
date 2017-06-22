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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.actions.ActionRegistry;
import org.smartdata.actions.HelloAction;
import org.smartdata.common.CmdletState;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.common.cmdlet.CmdletInfo;
import org.smartdata.metastore.MetaStore;
import org.smartdata.server.engine.cmdlet.CmdletDispatcher;
import org.smartdata.server.engine.cmdlet.message.ActionStatusReport;
import org.smartdata.server.engine.cmdlet.message.CmdletStatusUpdate;
import org.smartdata.server.engine.cmdlet.message.LaunchAction;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.cmdlet.message.StatusMessage;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
//Todo: 1. check file lock
public class CmdletManager extends AbstractService {
  private final Logger LOG = LoggerFactory.getLogger(CmdletManager.class);
  private ScheduledExecutorService executorService;
  private CmdletDispatcher dispatcher;
  private MetaStore metaStore;
  private AtomicLong maxActionId;
  private AtomicLong maxCmdletId;

  private Queue<CmdletInfo> pendingCmdlet;
  private Set<String> submittedCmdlets;
  private List<Long> runningCmdlets;
  private Map<Long, CmdletInfo> idToCmdlets;
  private Map<Long, ActionInfo> idToActions;

  public CmdletManager(ServerContext context) {
    super(context);

    //this.metaStore = context.getMetaStore();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.dispatcher = new CmdletDispatcher(this);
    this.runningCmdlets = new ArrayList<>();
    this.submittedCmdlets = new HashSet<>();
    this.pendingCmdlet = new LinkedBlockingQueue<>();
    this.idToCmdlets = new ConcurrentHashMap<>();
    this.idToActions = new ConcurrentHashMap<>();
  }

  @Override
  public void init() throws IOException {
    try {
      maxActionId = new AtomicLong(metaStore.getMaxActionId());
      maxCmdletId = new AtomicLong(metaStore.getMaxCmdletId());
    } catch (Exception e) {
      LOG.error("DB Connection error! Get Max CommandId/ActionId fail!", e);
      throw new IOException(e);
    }
  }

  @Override
  public void start() throws IOException {
    this.executorService.scheduleAtFixedRate(
        new ScheduleTask(this.dispatcher), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    this.executorService.shutdown();
  }

  public long submitCmdlet(String cmdlet) throws IOException {
    LOG.debug(String.format("Received Cmdlet -> [ %s ]", cmdlet));
    if (this.submittedCmdlets.contains(cmdlet)) {
      throw new IOException("Duplicate Cmdlet found, submit canceled!");
    }
    try {
      CmdletDescriptor cmdletDescriptor = CmdletDescriptor.fromCmdletString(cmdlet);
      return submitCmdlet(cmdletDescriptor);
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  public long submitCmdlet(CmdletDescriptor cmdletDescriptor) throws IOException {
    LOG.debug(String.format("Received Cmdlet -> [ %s ]", cmdletDescriptor.getCmdletString()));
    if (this.submittedCmdlets.contains(cmdletDescriptor.getCmdletString())) {
      throw new IOException("Duplicate Cmdlet found, submit canceled!");
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
    for (ActionInfo actionInfo : actionInfos) {
      cmdletInfo.addAction(actionInfo.getActionId());
    }
    for (int index = 0; index < cmdletDescriptor.actionSize(); index++) {
      if (!ActionRegistry.instance().checkAction(cmdletDescriptor.getActionName(index))) {
        throw new IOException(
          String.format("Submit Command %s error! Action names are not correct!", cmdletInfo));
      }
    }
    try {
      metaStore.insertCmdletTable(cmdletInfo);
      metaStore.insertActionsTable(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
    } catch (SQLException e) {
      LOG.error("Submit Command {} to DB error!", cmdletInfo);
      try {
        metaStore.deleteCmdlet(cmdletInfo.getCid());
      } catch (SQLException e1) {
        LOG.error("Delete Command {} rom DB error! {}", cmdletInfo, e);
      }
      throw new IOException(e);
    }
    this.pendingCmdlet.add(cmdletInfo);
    this.idToCmdlets.put(cmdletInfo.getCid(), cmdletInfo);
    this.submittedCmdlets.add(cmdletDescriptor.getCmdletString());
    for (ActionInfo actionInfo : actionInfos) {
      this.idToActions.put(actionInfo.getActionId(), actionInfo);
    }
    return cmdletInfo.getCid();
  }

  private synchronized List<ActionInfo> createActionInfos(CmdletDescriptor cmdletDescriptor, long cid) throws IOException {
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

  public synchronized void updateStatus(StatusMessage status) {
    if (status instanceof CmdletStatusUpdate) {
      onCmdletStatusUpdate((CmdletStatusUpdate) status);
    } else if (status instanceof ActionStatusReport) {
      onActionStatusReport((ActionStatusReport) status);
    }
  }

  private void onCmdletStatusUpdate(CmdletStatusUpdate statusUpdate) {

  }

  private void onActionStatusReport(ActionStatusReport report) {

  }

  int num = 0;
  public LaunchCmdlet getNextCmdletToRun() throws IOException {
    num +=1;
    List<LaunchAction> actions = new ArrayList<>();
    Map<String, String> args = new HashMap<>();
    args.put(HelloAction.PRINT_MESSAGE, "this is the message " + num);
    actions.add(new LaunchAction(101L, "hello", args));
    LaunchCmdlet cmdlet = new LaunchCmdlet(num, actions);
    if (num < 10) {
      return cmdlet;
    } else {
      return null;
    }
  }

  public CmdletInfo getCmdletInfo(long cid) throws IOException {
    if (idToCmdlets.containsKey(cid)) {
      return idToCmdlets.get(cid);
    }
    try {
      List<CmdletInfo> infos = metaStore.getCmdletsTableItem(String.format("= %d", cid),
        null, null);
      if (infos != null && infos.size() > 0) {
        return infos.get(0);
      } else {
        return null;
      }
    } catch (SQLException e) {
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
    } catch (SQLException e) {
      LOG.error("List CmdletInfo from DB error! Conditions rid {}, {}", rid, e);
      throw new IOException(e);
    }
    for (CmdletInfo info : idToCmdlets.values()) {
      if (info.getRid() == rid && info.getState().equals(cmdletState)) {
        result.add(info);
      }
    }
    return result;
  }

  public void activateCmdlet(long cid) throws IOException {
  }

  public void disableCmdlet(long cid) throws IOException {
    if (this.idToCmdlets.containsKey(cid)) {
      CmdletInfo info = idToCmdlets.get(cid);
      if (pendingCmdlet.contains(info)) {
        pendingCmdlet.remove(info);
        info.setState(CmdletState.DISABLED);
      }
      if (runningCmdlets.contains(cid)) {
        dispatcher.stop(cid);
      }
      flushCmdletInfo(info);
    }
  }

  public void deleteCmdlet(long cid) throws IOException {

  }

  public ActionInfo getActionInfo(long actionID) throws IOException {
    if (this.idToActions.containsKey(actionID)) {
      return this.idToActions.get(actionID);
    }
    try {
      return metaStore.getActionsTableItem(String.format("== %d ", actionID), null).get(0);
    } catch (SQLException e) {
      LOG.error("Get ActionInfo of {} from DB error! {}", actionID, e);
      throw new IOException(e);
    }
  }

  //Todo: move this function out of CmdletManager
  public List<ActionDescriptor> listActionsSupported() throws IOException {
    //TODO add more information for list ActionDescriptor
    ArrayList<ActionDescriptor> actionDescriptors = new ArrayList<>();
    for (String name : ActionRegistry.instance().namesOfAction()) {
      actionDescriptors.add(new ActionDescriptor(name,
        name, "", ""));
    }
    return actionDescriptors;
  }

  public List<ActionInfo> listNewCreatedActions(int maxNumActions) throws IOException {
    return null;
  }

  private void flushCmdletInfo(CmdletInfo info) {
  }

  private class ScheduleTask implements Runnable {
    private final CmdletDispatcher dispatcher;

    public ScheduleTask(CmdletDispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    public void run() {
      while (this.dispatcher.canDispatchMore()) {
        try {
          LaunchCmdlet launchCmdlet = getNextCmdletToRun();
          if (launchCmdlet == null) {
            break;
          } else {
            this.dispatcher.dispatch(launchCmdlet);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
