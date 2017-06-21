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
package org.smartdata.server.cmdlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.actions.ActionRegistry;
import org.smartdata.actions.HelloAction;
import org.smartdata.common.CmdletState;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.common.cmdlet.CmdletInfo;
import org.smartdata.server.ServerContext;
import org.smartdata.server.cmdlet.message.LaunchAction;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.cmdlet.message.StatusMessage;
import org.smartdata.server.metastore.DBAdapter;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

//Todo: 1. check file lock
public class CmdletManager extends AbstractService {
  private final Logger LOG = LoggerFactory.getLogger(CmdletManager.class);
  private ScheduledExecutorService executorService;
  private CmdletDispatcher dispatcher;
  private Queue<CmdletInfo> pendingCmdlet;
  private Map<String, Long> submittedCmdlets;
  private DBAdapter adapter;
  private AtomicLong maxActionId;
  private AtomicLong maxCmdletId;

  public CmdletManager(ServerContext context) {
    super(context);

    this.adapter = context.getDbAdapter();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.dispatcher = new CmdletDispatcher(this);
    this.submittedCmdlets = new ConcurrentHashMap<>();
    this.pendingCmdlet = new LinkedBlockingQueue<>();
  }

  @Override
  public void init() throws IOException {
      try {
        maxActionId = new AtomicLong(adapter.getMaxActionId());
        maxCmdletId = new AtomicLong(adapter.getMaxCmdletId());
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
    if (this.submittedCmdlets.containsKey(cmdlet)) {
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
    if (this.submittedCmdlets.containsKey(cmdletDescriptor.getCmdletString())) {
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
      adapter.insertCmdletTable(cmdletInfo);
      adapter.insertActionsTable(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
    } catch (SQLException e) {
      LOG.error("Submit Command {} to DB error!", cmdletInfo);
      try {
        adapter.deleteCmdlet(cmdletInfo.getCid());
      } catch (SQLException e1) {
        LOG.error("Recover/Delete Command {} rom DB error! {}", cmdletInfo, e);
      }
      throw new IOException(e);
    }
    this.submittedCmdlets.put(cmdletDescriptor.getCmdletString(), cmdletInfo.getCid());
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

  public synchronized void updateStatue(StatusMessage status) {
    System.out.println("Got message " + status);
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
