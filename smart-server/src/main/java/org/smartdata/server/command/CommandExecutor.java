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
package org.smartdata.server.command;

import com.google.common.annotations.VisibleForTesting;
import org.smartdata.SmartContext;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.actions.ActionStatus;
import org.smartdata.common.actions.ActionInfoComparator;
import org.smartdata.common.actions.ActionType;
import org.smartdata.actions.SmartAction;
import org.smartdata.actions.hdfs.HdfsAction;
import org.smartdata.common.CommandState;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.ModuleSequenceProto;
import org.smartdata.server.SmartServer;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.actions.ActionRegistry;

import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Schedule and execute commands passed down.
 */
public class CommandExecutor implements Runnable, ModuleSequenceProto {
  static final Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

  private ArrayList<Set<Long>> cmdsInState = new ArrayList<>();
  private Map<Long, CommandInfo> cmdsAll = new ConcurrentHashMap<>();
  // TODO replace with concurrentSet or MAP
  private Set<CmdTuple> statusCache;
  private Daemon commandExecutorThread;
  private CommandPool commandPool;
  private Map<Long, SmartAction> actionPool;
  private DBAdapter adapter;
  private ActionRegistry actionRegistry;
  private SmartServer ssm;
  private SmartContext smartContext;
  private boolean running;
  private long currentActionId;

  public CommandExecutor(SmartServer ssm) {
    this.ssm = ssm;
    actionRegistry = ActionRegistry.instance();
    statusCache = new HashSet<>();
    for (CommandState s : CommandState.values()) {
      cmdsInState.add(s.getValue(), new HashSet<Long>());
    }
    smartContext = new SmartContext();
    actionPool = new HashMap<>();
    commandPool = new CommandPool();
    running = false;
    // TODO recovery ActionID
    currentActionId = 0;
  }

  public CommandExecutor(SmartServer ssm, SmartConf conf) {
    this(ssm);
    smartContext = new SmartContext(conf);
  }

  public boolean init(DBAdapter adapter) throws IOException {
    if (adapter != null) {
      this.adapter = adapter;
      return true;
    }
    return false;
  }

  /**
   * Start CommandExecutor.
   */
  public boolean start() throws IOException {
    // TODO add recovery code
    commandExecutorThread = new Daemon(this);
    commandExecutorThread.setName(this.getClass().getCanonicalName());
    commandExecutorThread.start();
    running = true;
    return true;
  }

  /**
   * Stop CommandExecutor
   */
  public void stop() throws IOException {
    running = false;
    // Update Status
    batchCommandStatusUpdate();
  }

  public void join() throws IOException {
    try {
      if (commandPool != null) {
        commandPool.stop();
      }
    } catch (Exception e) {
      LOG.error("Shutdown MoverPool/CommandPool Error!");
      throw new IOException(e);
    }
    // Set all thread handle to null
    commandPool = null;
    commandExecutorThread = null;
  }

  @Override
  public void run() {
    while (running) {
      try {
        // control the commands that executed concurrently
        if (commandPool == null) {
          LOG.error("Thread Init/Start Error!");
        }
        // TODO: use configure value
        if (commandPool.size() <= 5) {
          Command toExec = schedule();
          if (toExec != null) {
            toExec.setScheduleToExecuteTime(Time.now());
            commandPool.execute(toExec);
          } else {
            Thread.sleep(1000);
          }
        } else {
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        if (!running) {
          break;
        }
      } catch (IOException e) {
        LOG.error("Schedule error!");
        LOG.error(e.getMessage());
      }
    }
  }

  public CommandInfo getCommandInfo(long cid) throws IOException {
    if (cmdsAll.containsKey(cid)) {
      return cmdsAll.get(cid);
    }
    List<CommandInfo> ret = null;
    try {
      ret = adapter.getCommandsTableItem(String.format("= %d", cid),
          null, null);
    } catch (SQLException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
    if (ret != null) {
      return ret.get(0);
    }
    return null;
  }

  public List<CommandInfo> listCommandsInfo(long rid,
                                            CommandState commandState) throws IOException {
    List<CommandInfo> retInfos = new ArrayList<>();
    // Get from DB
    try {
      if (rid != -1) {
        retInfos.addAll(adapter.getCommandsTableItem(null,
            String.format("= %d", rid), commandState));
      } else {
        retInfos.addAll(adapter.getCommandsTableItem(null,
            null, commandState));
      }
    } catch (SQLException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
    // Get from Cache if commandState != CommandState.PENDING
    if (commandState != CommandState.PENDING) {
      for (Iterator<CommandInfo> iter = cmdsAll.values().iterator(); iter.hasNext(); ) {
        CommandInfo cmdinfo = iter.next();
        if (cmdinfo.getState() == commandState && cmdinfo.getRid() == rid) {
          retInfos.add(cmdinfo);
        }
      }
    }
    return retInfos;
  }

  public void activateCommand(long cid) throws IOException {
    if (inCache(cid)) {
      return;
    }
    if (inUpdateCache(cid)) {
      return;
    }
    CommandInfo cmdinfo = getCommandInfo(cid);
    if (cmdinfo == null || cmdinfo.getState() == CommandState.DONE) {
      return;
    }
    LOG.info("Activate Command {}", cid);
    cmdinfo.setState(CommandState.PENDING);
    addToPending(cmdinfo);
  }

  public void disableCommand(long cid) throws IOException {
    // Remove from Cache
    if (inCache(cid)) {
      LOG.info("Disable Command {}", cid);
      // Command is finished, then return
      if (inUpdateCache(cid)) {
        return;
      }
      CommandInfo cmdinfo = cmdsAll.get(cid);
      cmdinfo.setState(CommandState.DISABLED);
      // Disable this command in cache
      if (inExecutingList(cid)) {
        // Remove from Executing queue
        removeFromExecuting(cid, cmdinfo.getRid(), cmdinfo.getState());
        // Kill thread
        commandPool.deleteCommand(cid);
      } else {
        // Remove from Pending queue
        cmdsInState.get(CommandState.PENDING.getValue()).remove(cid);
      }
      // Mark as cancelled, this status will be update to DB
      // in next batch update
      synchronized (statusCache) {
        statusCache.add(new CmdTuple(cid, cmdinfo.getRid(),
            CommandState.DISABLED));
      }
    }
  }

  public void deleteCommand(long cid) throws IOException {
    // Delete from DB
    // Remove from Cache
    if (inCache(cid)) {
      // Command is finished, then return
      CommandInfo cmdinfo = cmdsAll.get(cid);
      // Disable this command in cache
      if (inExecutingList(cid)) {
        // Remove from Executing queue
        removeFromExecuting(cid, cmdinfo.getRid(), cmdinfo.getState());
        // Kill thread
        commandPool.deleteCommand(cid);
      } else if (inUpdateCache(cid)) {
        removeFromUpdateCache(cid);
      } else {
        // Remove from Pending queue
        cmdsInState.get(CommandState.PENDING.getValue()).remove(cid);
      }
      // Mark as cancelled, this status will be update to DB
      // in next batch update
      cmdsAll.remove(cid);
    }
    try {
      adapter.deleteCommand(cid);
    } catch (SQLException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
  }

  public ActionInfo getActionInfo(long actionID) {
    SmartAction smartAction =  actionPool.get(actionID);
    ActionStatus status = smartAction.getActionStatus();
    return new ActionInfo(status.getId(),
        0, smartAction.getName(), smartAction.getArguments(), status.getResultPrintStream().toString(),
        status.getLogPrintStream().toString(), status.isSuccessful(), status.getStartTime(),
        status.isSuccessful(), status.getRunningTime(), status.getPercentage());
  }

  /**
   * List actions supported in SmartServer.
   *
   * @return
   * @throws IOException
   */
  public List<ActionDescriptor> listActionsSupported() throws IOException {
    //TODO add more information for list ActionDescriptor
    ArrayList<ActionDescriptor> actionDescriptors = new ArrayList<>();
    for (String name : ActionRegistry.instance().namesOfAction()) {
      actionDescriptors.add(new ActionDescriptor(name, name, "", ""));
    }
    return actionDescriptors;
  }

  private void addToPending(CommandInfo cmdinfo) throws IOException {
    Set<Long> cmdsPending = cmdsInState.get(CommandState.PENDING.getValue());
    cmdsAll.put(cmdinfo.getCid(), cmdinfo);
    cmdsPending.add(cmdinfo.getCid());
  }

  public int cacheSize() {
    return cmdsAll.size();
  }

  public boolean inCache(long cid) throws IOException {
    return cmdsAll.containsKey(cid);
  }

  public boolean inExecutingList(long cid) throws IOException {
    Set<Long> cmdsExecuting = cmdsInState
        .get(CommandState.EXECUTING.getValue());
    return cmdsExecuting.contains(cid);
  }

  public boolean inPendingList(long cid) throws IOException {
    Set<Long> cmdsPending = cmdsInState.get(CommandState.PENDING.getValue());
    LOG.info("Size of Pending = {}", cmdsPending.size());
    return cmdsPending.contains(cid);
  }

  public boolean inUpdateCache(long cid) throws IOException {
    if (statusCache.size() == 0) {
      return false;
    }
    for (CmdTuple ct : statusCache) {
      if (ct.cid == cid) {
        return true;
      }
    }
    return false;
  }

  private void removeFromUpdateCache(long cid) throws IOException {
    if (statusCache.size() == 0) {
      return;
    }
    synchronized (statusCache) {
      for (Iterator<CmdTuple> iter = statusCache.iterator(); iter.hasNext(); ) {
        CmdTuple ct = iter.next();
        if (ct.cid == cid) {
          iter.remove();
          break;
        }
      }
    }
  }

  /**
   * Get command to for execution.
   *
   * @return
   */
  private synchronized Command schedule() throws IOException {
    // currently FIFO
    // List<Long> cmds = getCommands(CommandState.PENDING);
    Set<Long> cmdsPending = cmdsInState
        .get(CommandState.PENDING.getValue());
    Set<Long> cmdsExecuting = cmdsInState
        .get(CommandState.EXECUTING.getValue());
    if (cmdsPending.size() == 0) {
      // Put them into cmdsAll and cmdsInState
      if (statusCache.size() != 0) {
        batchCommandStatusUpdate();
      }
      List<CommandInfo> dbcmds = getPendingCommandsFromDB();
      if (dbcmds == null) {
        return null;
      }
      for (CommandInfo c : dbcmds) {
        // if command alread in update cache or queue then skip
        if (cmdsAll.containsKey(c.getCid())) {
          continue;
        }
        cmdsAll.put(c.getCid(), c);
        cmdsPending.add(c.getCid());
      }
      if (cmdsPending.size() == 0) {
        return null;
      }
    }
    // TODO Replace FIFO
    // Currently only get and run the first cmd
    long curr = cmdsPending.iterator().next();
    Command ret = getCommandFromCmdInfo(cmdsAll.get(curr));
    cmdsPending.remove(curr);
    cmdsExecuting.add(curr);
    ret.setState(CommandState.EXECUTING);
    return ret;
  }

  private SmartAction createAction(String name) {
    SmartAction smartAction = actionRegistry.createAction(name);
    smartAction.setContext(smartContext);
    if (smartAction instanceof HdfsAction) {
      ((HdfsAction) smartAction).setDfsClient(ssm.getDFSClient());
    }
    smartAction.getActionStatus().setId(currentActionId);
    currentActionId++;
    return smartAction;
  }

/*  private SmartAction[] createActionsFromStringJson(String jsonString) throws IOException {
    List<Map<String, String>> actionMaps =
            JsonUtil.toArrayListMap(jsonString);
    List<SmartAction> actions = new ArrayList<>();
    SmartAction current;
    for(Map<String, String> actionMap: actionMaps) {
      // New action
      String[] args = {actionMap.get("_FILE_PATH_"), actionMap.get("_STORAGE_POLICY_")};
      current = createAction(actionMap.get("_NAME_"));
      if (current == null) {
        LOG.error("New Action Instance from {} error!", actionMap.get("_NAME_"));
        throw new IOException();
      }
      current.setContext(smartContext);
      current.init(args);
      actions.add(current);
    }
    return actions.toArray(new SmartAction[actionMaps.size()]);
  }*/

  private SmartAction[] createActionsFromParameters(String commandDescriptorString) throws IOException {
    CommandDescriptor commandDescriptor = null;
    try {
      commandDescriptor = CommandDescriptor.fromCommandString(commandDescriptorString);
    } catch (ParseException e) {
      LOG.error("Command Descriptor String Wrong format! ", e.getMessage());
    }
    return createActionsFromParameters(commandDescriptor);
  }

  @VisibleForTesting
  SmartAction[] createActionsFromParameters(CommandDescriptor commandDescriptor) throws IOException {
    if (commandDescriptor == null) {
      return null;
    }
    // commandDescriptor.();
    List<SmartAction> actions = new ArrayList<>();
    SmartAction current;
    for (int index = 0; index < commandDescriptor.size(); index++) {
      current = createAction(commandDescriptor.getActionName(index));
      actionPool.put(current.getActionStatus().getId(), current);
      if (current == null) {
        LOG.error("New Action Instance from {} error!", commandDescriptor.getActionName(index));
      }
      current.setContext(smartContext);
      current.init(commandDescriptor.getActionArgs(index));
      actions.add(current);
    }
    return actions.toArray(new SmartAction[commandDescriptor.size()]);
  }

  public synchronized long submitCommand(String commandDescriptorString) throws IOException {
    CommandDescriptor commandDescriptor;
    try {
      commandDescriptor = CommandDescriptor.fromCommandString(commandDescriptorString);
    } catch (ParseException e) {
      LOG.error("Command Descriptor String Wrong format! ", e.getMessage());
      throw new IOException(e);
    }
    return submitCommand(commandDescriptor);
  }

  public synchronized long submitCommand(CommandDescriptor commandDescriptor) throws IOException {
    if (commandDescriptor == null) {
      LOG.error("Command Descriptor!");
      throw new IOException();
      // return -1;
    }
    long submitTime = System.currentTimeMillis();
    CommandInfo cmdinfo = new CommandInfo(0, commandDescriptor.getRuleId(),
        ActionType.ArchiveFile, CommandState.PENDING,
        commandDescriptor.getCommandString(), submitTime, submitTime);
    return submitCommand(cmdinfo);
  }

  public synchronized long submitCommand(CommandInfo cmd) throws IOException {
    try {
      if (adapter.insertCommandTable(cmd)) {
        cmdsAll.put(cmd.getCid(), cmd);
        cmdsInState.get(CommandState.PENDING.getValue()).add(cmd.getCid());
        return cmd.getCid();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new IOException();
    }
    return -1;
  }

  private Command getCommandFromCmdInfo(CommandInfo cmdinfo) throws IOException {
    // New Command
    Command cmd = new Command(createActionsFromParameters(cmdinfo.getParameters()),
        new Callback());
    cmd.setParameters(cmdinfo.getParameters());
    cmd.setId(cmdinfo.getCid());
    cmd.setRuleId(cmdinfo.getRid());
    cmd.setState(cmdinfo.getState());
    // Init action
    return cmd;
  }

  public List<ActionInfo> listNewCreatedActions(int maxNumActions) throws IOException {
    ArrayList<ActionInfo> actionInfos = new ArrayList<>();
    boolean flag = true;
    for (Command cmd : commandPool.getcommands()) {
      long cmdId = cmd.getId();
      for (SmartAction smartAction : cmd.getSmartActions()) {
        ActionStatus status = smartAction.getActionStatus();
        actionInfos.add(new ActionInfo(status.getId(),
            cmdId, smartAction.getName(), smartAction.getArguments(), status.getResultPrintStream().toString(),
            status.getLogPrintStream().toString(), status.isSuccessful(), status.getStartTime(),
            status.isSuccessful(), status.getRunningTime(), status.getPercentage()));
      }
    }
    // Sort and get top maxNumActions
    Collections.sort(actionInfos, new ActionInfoComparator());
    if (maxNumActions >= actionInfos.size()) {
      return actionInfos;
    } else {
      return actionInfos.subList(0, maxNumActions);
    }
  }

  public List<CommandInfo> getPendingCommandsFromDB() throws IOException {
    // Get Pending cmds from DB
    try {
      return adapter.getCommandsTableItem(null, null, CommandState.PENDING);
    } catch (SQLException e) {
      // TODO: handle this issue
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
  }

  public Long[] getCommands(CommandState state) {
    Set<Long> cmds = cmdsInState.get(state.getValue());
    return cmds.toArray(new Long[cmds.size()]);
  }

  public synchronized void batchCommandStatusUpdate() throws IOException {
    LOG.info("INFO Number of Caches = {}", statusCache.size());
    LOG.info("INFO Number of Actions = {}", cmdsAll.size());
    if (statusCache.size() == 0) {
      return;
    }
    synchronized (statusCache) {
      for (Iterator<CmdTuple> iter = statusCache.iterator(); iter.hasNext(); ) {
        CmdTuple ct = iter.next();
        cmdsAll.remove(ct.cid);
        try {
          adapter.updateCommandStatus(ct.cid, ct.rid, ct.state);
        } catch (SQLException e) {
          // TODO: handle this isssue
          LOG.error(e.getMessage());
          throw new IOException(e);
        }
        iter.remove();
      }
    }
  }

  public class CmdTuple {
    public long cid;
    public long rid;
    public CommandState state;

    public CmdTuple(long cid, long rid, CommandState state) {
      this.cid = cid;
      this.rid = rid;
      this.state = state;
    }

    public String toString() {
      return String.format("Rule-%d-cmd-%d", cid, rid);
    }
  }

  private void removeFromExecuting(long cid, long rid, CommandState state) {
    Set<Long> cmdsExecuting = cmdsInState.get(CommandState.EXECUTING.getValue());
    if (cmdsExecuting.size() == 0) {
      return;
    }
    cmdsExecuting.remove(cid);
  }

  public class Callback {

    public void complete(long cid, long rid, CommandState state) {
      commandExecutorThread.interrupt();
      // Update State in Cache
      if (cmdsAll.get(cid) == null) {
        LOG.error("Command is null!");
      }
      LOG.info("Command {} finished!", cmdsAll.get(cid));
      // Mark commandInfo as DONE
      cmdsAll.get(cid).setState(state);
      // Mark command as DONE
      commandPool.setFinished(cid, state);
      LOG.info("Command {}", state.toString());
      synchronized (statusCache) {
        statusCache.add(new CmdTuple(cid, rid, state));
      }
      removeFromExecuting(cid, rid, state);
      try {
        commandPool.deleteCommand(cid);
      } catch (Exception e) {
        LOG.error("Shutdown Command {} Error!", cid);
      }
    }
  }
}
