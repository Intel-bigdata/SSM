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
import org.smartdata.actions.ActionRegistry;
import org.smartdata.actions.PrintAction;
import org.smartdata.common.CmdletState;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.common.cmdlet.CmdletInfo;
import org.smartdata.server.Service;
import org.smartdata.server.cmdlet.message.LaunchAction;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;
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
public class CmdletManager implements Service {
  private final Logger LOG = LoggerFactory.getLogger(CmdletManager.class);
  private ScheduledExecutorService executorService;
  private CmdletDispatcher dispatcher;
  private Queue<CmdletInfo> pendingCmdlet;
  private Map<String, Long> submittedCommand;
  private DBAdapter adapter;
  private AtomicLong maxActionId;
  private AtomicLong maxCommandId;

  public CmdletManager() {
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.dispatcher = new CmdletDispatcher();
    this.submittedCommand = new ConcurrentHashMap<>();
    this.pendingCmdlet = new LinkedBlockingQueue<>();
  }

  @Override
  public boolean init(DBAdapter adapter) throws IOException {
    if (adapter != null) {
      this.adapter = adapter;
      try {
        maxActionId = new AtomicLong(adapter.getMaxActionId());
        maxCommandId = new AtomicLong(adapter.getMaxCmdletId());
      } catch (Exception e) {
        LOG.error("DB Connection error! Get Max CommandId/ActionId fail!", e);
        throw new IOException(e);
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean start() throws IOException, InterruptedException {
    this.executorService.scheduleAtFixedRate(
        new ScheduleTask(this.dispatcher), 1000, 1000, TimeUnit.MILLISECONDS);
    return true;
  }

  @Override
  public void stop() throws IOException {
    this.executorService.shutdown();
  }

  @Override
  public void join() throws IOException {

  }

  public long submitCommand(String command) throws IOException {
    LOG.debug(String.format("Received Command -> [ %s ]", command));
    if (this.submittedCommand.containsKey(command)) {
      throw new IOException("Duplicate Command found, submit canceled!");
    }
    try {
      CmdletDescriptor commandDescriptor = CmdletDescriptor.fromCmdletString(command);
      return submitCommand(commandDescriptor);
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  public long submitCommand(CmdletDescriptor commandDescriptor) throws IOException {
    LOG.debug(String.format("Received Command -> [ %s ]", commandDescriptor.getCmdletString()));
    if (this.submittedCommand.containsKey(commandDescriptor.getCmdletString())) {
      throw new IOException("Duplicate Command found, submit canceled!");
    }
    long submitTime = System.currentTimeMillis();
    CmdletInfo cmdletInfo =
      new CmdletInfo(
        maxCommandId.getAndIncrement(),
        commandDescriptor.getRuleId(),
        CmdletState.PENDING,
        commandDescriptor.getCmdletString(),
        submitTime,
        submitTime);
    List<ActionInfo> actionInfos = createActionInfos(commandDescriptor, cmdletInfo.getCid());
    for (ActionInfo actionInfo : actionInfos) {
      cmdletInfo.addAction(actionInfo.getActionId());
    }
    for (int index = 0; index < commandDescriptor.actionSize(); index++) {
      if (!ActionRegistry.instance().checkAction(commandDescriptor.getActionName(index))) {
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
    this.submittedCommand.put(commandDescriptor.getCmdletString(), cmdletInfo.getCid());
    return cmdletInfo.getCid();
  }

  private synchronized List<ActionInfo> createActionInfos(CmdletDescriptor commandDescriptor, long cid) throws IOException {
    List<ActionInfo> actionInfos = new ArrayList<>();
    for (int index = 0; index < commandDescriptor.actionSize(); index++) {
      Map<String, String> args = commandDescriptor.getActionArgs(index);
      ActionInfo actionInfo =
          new ActionInfo(
              maxActionId.getAndIncrement(),
              cid,
              commandDescriptor.getActionName(index),
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

  int num = 0;

  public LaunchCmdlet getNextCommandToRun() throws IOException {
    num +=1;
    List<LaunchAction> actions = new ArrayList<>();
    Map<String, String> args = new HashMap<>();
    args.put(PrintAction.PRINT_MESSAGE, "this is the message " + num);
    actions.add(new LaunchAction(101L, "print", args));
    LaunchCmdlet cmdlet = new LaunchCmdlet(0, actions);
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
          LaunchCmdlet commandInfo = getNextCommandToRun();
          if (commandInfo == null) {
            break;
          } else {
            this.dispatcher.dispatch(commandInfo);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
