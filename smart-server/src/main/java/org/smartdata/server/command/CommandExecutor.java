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
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.smartdata.SmartContext;
import org.smartdata.client.SmartDFSClient;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.actions.ActionStatus;
import org.smartdata.common.actions.ActionInfoComparator;
import org.smartdata.actions.SmartAction;
import org.smartdata.actions.hdfs.HdfsAction;
import org.smartdata.common.CommandState;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.server.ModuleSequenceProto;
import org.smartdata.server.SmartServer;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.actions.ActionRegistry;

import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private Map<String, Long> commandHashSet;
  private Map<Long, SmartAction> actionPool;
  private DBAdapter adapter;
  private ActionRegistry actionRegistry;
  private SmartServer ssm;
  private SmartContext smartContext;
  private boolean running;
  private long maxActionId;
  private long maxCommandId;
  private Configuration conf;

  public CommandExecutor(SmartServer ssm) {
    this.ssm = ssm;
    actionRegistry = ActionRegistry.instance();
    statusCache = new HashSet<>();
    for (CommandState s : CommandState.values()) {
      cmdsInState.add(s.getValue(), new HashSet<Long>());
    }
    smartContext = new SmartContext();
    commandPool = new CommandPool();
    actionPool = new ConcurrentHashMap<>();
    commandHashSet = new ConcurrentHashMap<>();
    running = false;
  }

  public CommandExecutor(SmartServer ssm, SmartConf conf) {
    this(ssm);
    smartContext = new SmartContext(conf);
    this.conf = conf;
  }

  public boolean init(DBAdapter adapter) throws IOException {
    if (adapter != null) {
      this.adapter = adapter;
      try {
        maxActionId = adapter.getMaxActionId();
        maxCommandId = adapter.getMaxCommandId();
      } catch (Exception e) {
        maxActionId = 1;
        LOG.error("DB Connection error! Get Max CommandId/ActionId fail!", e);
        throw new IOException(e);
      }
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
    adapter = null;
    ssm = null;
    smartContext = null;
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
        LOG.error("Schedule error!", e);
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
      LOG.error("Get CommandInfo with ID {} from DB error! {}", cid, e);
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
      LOG.error("List CommandInfo from DB error! Conditions rid {}, {}", rid, e);
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
        removeFromExecuting(cid, cmdinfo.getRid());
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
        removeFromExecuting(cid, cmdinfo.getRid());
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
      LOG.error("Delete Command {} from DB error! {}", cid, e);
      throw new IOException(e);
    }
  }

  public ActionInfo getActionInfo(long actionID) throws IOException {
    ActionInfo actionInfo = null;
    ActionInfo dbActionInfo = null;
    try {
      dbActionInfo = adapter.getActionsTableItem(
          String.format("== %d ", actionID), null).get(0);
    } catch (SQLException e) {
      LOG.error("Get ActionInfo of {} from DB error! {}",
          actionID, e);
      throw new IOException(e);
    }
    if (dbActionInfo.isFinished()) {
      return dbActionInfo;
    }
    SmartAction smartAction = actionPool.get(actionID);
    if (smartAction != null) {
      actionInfo = createActionInfoFromAction(smartAction, 0);
    }
    if (actionInfo == null) {
      return dbActionInfo;
    }
    actionInfo.setCommandId(dbActionInfo.getCommandId());
    return actionInfo;
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
      actionDescriptors.add(new ActionDescriptor(name,
          name, "", ""));
    }
    return actionDescriptors;
  }

  public boolean isActionSupported(String actionName) {
    return actionRegistry.checkAction(actionName);
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
    CommandInfo commandInfo = cmdsAll.get(curr);
    Command ret = getCommandFromCmdInfo(commandInfo);
    cmdsPending.remove(curr);
    if (ret == null) {
      // Create Command from CommandInfo Fail
      LOG.error("Create Command from CommandInfo {}", commandInfo);
      statusCache.add(new CmdTuple(commandInfo.getCid(), commandInfo.getRid(), CommandState.DISABLED));
      return null;
    }
    cmdsExecuting.add(curr);
    ret.setState(CommandState.EXECUTING);
    return ret;
  }

  private SmartAction createSmartAction(ActionInfo actionInfo) throws IOException {
    SmartAction smartAction = actionRegistry.createAction(actionInfo.getActionName());
    if (smartAction == null) {
      return null;
    }
    smartAction.setContext(smartContext);
    smartAction.setArguments(actionInfo.getArgs());
    if (smartAction instanceof HdfsAction) {
      ((HdfsAction) smartAction).setDfsClient(
          new SmartDFSClient(ssm.getNamenodeURI(),
              smartContext.getConf(), getRpcServerAddress()));
    }
    smartAction.getActionStatus().setId(actionInfo.getActionId());
    return smartAction;
  }

  private SmartAction createSmartAction(String name) throws IOException {
    SmartAction smartAction = actionRegistry.createAction(name);
    if (smartAction == null) {
      return null;
    }
    smartAction.setContext(smartContext);
    if (smartAction instanceof HdfsAction) {
      ((HdfsAction) smartAction).setDfsClient(
          new SmartDFSClient(ssm.getNamenodeURI(),
              smartContext.getConf(), getRpcServerAddress()));
    }
    smartAction.getActionStatus().setId(maxActionId);
    maxActionId++;
    return smartAction;
  }

  private InetSocketAddress getRpcServerAddress() {
    String[] strings = conf.get(SmartConfKeys.DFS_SSM_RPC_ADDRESS_KEY,
        SmartConfKeys.DFS_SSM_RPC_ADDRESS_DEFAULT).split(":");
    return new InetSocketAddress(strings[strings.length - 2]
        , Integer.parseInt(strings[strings.length - 1]));
  }

  private List<ActionInfo> createActionInfos(String commandDescriptorString, long cid) throws IOException {
    CommandDescriptor commandDescriptor = null;
    try {
      commandDescriptor = CommandDescriptor.fromCommandString(commandDescriptorString);
    } catch (ParseException e) {
      LOG.error("Command Descriptor {} String Wrong format! {}", commandDescriptorString, e);
    }
    return createActionInfos(commandDescriptor, cid);
  }


  @VisibleForTesting
  List<ActionInfo> createActionInfos(CommandDescriptor commandDescriptor, long cid) throws IOException {
    if (commandDescriptor == null) {
          return null;
    }
    List<ActionInfo> actionInfos = new ArrayList<>();
    ActionInfo current;
    try {
      for (int index = 0; index < commandDescriptor.size(); index++) {
        current = new ActionInfo(maxActionId, cid,
            commandDescriptor.getActionName(index),
            commandDescriptor.getActionArgs(index), "","",
            false,0,false, 0, 0);
        maxActionId++;
        actionInfos.add(current);
      }
    } catch (Exception e) {
      LOG.error("Create ActionInfos from CommandDescriptor {} fail! {}", commandDescriptor, e);
      return null;
    }
    return actionInfos;
  }

  public synchronized long submitCommand(String commandDescriptorString)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received Command -> [" + commandDescriptorString + "]");
    }
    if (commandHashSet.containsKey(commandDescriptorString)) {
      LOG.debug("Duplicate Command found, submit canceled!");
      throw new IOException();
      // return -1;
    }
    CommandDescriptor commandDescriptor;
    try {
      commandDescriptor = CommandDescriptor.fromCommandString(commandDescriptorString);
    } catch (ParseException e) {
      LOG.error("Command Descriptor {} Wrong format! {}", commandDescriptorString, e);
      throw new IOException(e);
    }
    return submitCommand(commandDescriptor);
  }

  public synchronized long submitCommand(CommandDescriptor commandDescriptor)
      throws IOException {
    if (commandDescriptor == null) {
      LOG.error("Command Descriptor!");
      throw new IOException();
      // return -1;
    }
    if (commandHashSet.containsKey(commandDescriptor.getCommandString())) {
      LOG.debug("Duplicate Command found, submit canceled!");
      throw new IOException();
      // return -1;
    }
    long submitTime = System.currentTimeMillis();
    CommandInfo cmdinfo = new CommandInfo(maxCommandId, commandDescriptor.getRuleId(),
        CommandState.PENDING, commandDescriptor.getCommandString(),
        submitTime, submitTime);
    maxCommandId ++;
    for (int index = 0; index < commandDescriptor.size(); index++) {
      if (!actionRegistry.checkAction(commandDescriptor.getActionName(index))) {
        LOG.error("Submit Command {} error! Action names are not correct!", cmdinfo);
        throw new IOException();
      }
    }
    return submitCommand(cmdinfo);
  }

  public synchronized long submitCommand(CommandInfo cmdinfo) throws IOException {
    long cid = cmdinfo.getCid();
    List<ActionInfo> actionInfos = createActionInfos(cmdinfo.getParameters(), cid);
    for (ActionInfo actionInfo: actionInfos) {
      cmdinfo.addAction(actionInfo.getActionId());
    }
    try {
      // Insert Command into DB
      commandHashSet.put(cmdinfo.getParameters(), cmdinfo.getCid());
      adapter.insertCommandTable(cmdinfo);
      // Insert Action into DB
      adapter.insertActionsTable(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
    } catch (SQLException e) {
      LOG.error("Submit Command {} to DB error! {}", cmdinfo, e);
      throw new IOException(e);
    }
    return cid;
  }

  private Command getCommandFromCmdInfo(CommandInfo cmdinfo)
      throws IOException {
    // New Command
    Command cmd;
    List<ActionInfo> actionInfos;
    try {
      actionInfos = adapter.getActionsTableItem(cmdinfo.getAids());
    } catch (SQLException e) {
      LOG.error("Get Actions from DB with IDs {} error!", cmdinfo.getAids());
      throw new IOException(e);
    }
    if (actionInfos == null || actionInfos.size() == 0){
      return null;
    }
    List<SmartAction> smartActions = new ArrayList<>();
    for (ActionInfo actionInfo: actionInfos) {
      SmartAction smartAction = createSmartAction(actionInfo);
      smartActions.add(smartAction);
      actionPool.put(actionInfo.getActionId(), smartAction);
    }
    if (smartActions.size() == 0) {
      return null;
    }
    cmd = new Command(smartActions.toArray(new SmartAction[smartActions.size()]),
        new Callback());
    cmd.setParameters(cmdinfo.getParameters());
    cmd.setId(cmdinfo.getCid());
    cmd.setRuleId(cmdinfo.getRid());
    cmd.setState(cmdinfo.getState());
    // Init action
    return cmd;
  }

  public List<ActionInfo> listNewCreatedActions(int maxNumActions)
      throws IOException {
    ArrayList<ActionInfo> actionInfos = new ArrayList<>();
    for (Command cmd : commandPool.getcommands()) {
      long cmdId = cmd.getId();
      for (SmartAction smartAction : cmd.getActions()) {
        actionInfos.add(createActionInfoFromAction(smartAction, cmdId));
      }
    }
    // Sort and get top maxNumActions
    Collections.sort(actionInfos, new ActionInfoComparator());
    if (maxNumActions <= actionInfos.size()) {
      return actionInfos.subList(0, maxNumActions);
    }
    // Get actions from Db
    int remainsAction = maxNumActions - actionInfos.size();
    try {
      actionInfos.addAll(adapter.getNewCreatedActionsTableItem(remainsAction));
    } catch (SQLException e) {
      LOG.error("Get Finished Actions from DB error", e);
      throw new IOException(e);
    }
    return actionInfos;
  }

  public List<CommandInfo> getPendingCommandsFromDB() throws IOException {
    // Get Pending cmds from DB
    try {
      return adapter.getCommandsTableItem(null,
          null, CommandState.PENDING);
    } catch (SQLException e) {
      LOG.error("Get Pending Commands From DB error!", e);
      throw new IOException(e);
    }
  }

  public Long[] getCommands(CommandState state) {
    Set<Long> cmds = cmdsInState.get(state.getValue());
    return cmds.toArray(new Long[cmds.size()]);
  }

  private ActionInfo createActionInfoFromAction(SmartAction smartAction,
      long cid) throws IOException {
    ActionStatus status = smartAction.getActionStatus();
    // Replace special character with
    return new ActionInfo(status.getId(),
        cid, smartAction.getName(),
        smartAction.getArguments(),
        StringEscapeUtils.escapeJava(status.getResultStream().toString("UTF-8")),
        StringEscapeUtils.escapeJava(status.getLogStream().toString("UTF-8")),
        status.isSuccessful(), status.getStartTime(),
        status.isFinished(), status.getFinishTime(),
        status.getPercentage());
  }

  private List<ActionInfo> getActionInfoFromCommand(long cid) throws IOException {
    ArrayList<ActionInfo> actionInfos = new ArrayList<>();
    Command cmd = commandPool.getCommand(cid);
    if (cmd == null) {
      return actionInfos;
    }
    for (SmartAction smartAction : cmd.getActions()) {
      actionInfos.add(createActionInfoFromAction(smartAction, cid));
    }
    return actionInfos;
  }

  public synchronized void batchCommandStatusUpdate() throws IOException {
    if (commandPool == null || adapter == null) {
      return;
    }
    LOG.info("INFO Number of Caches = {}", statusCache.size());
    LOG.info("INFO Number of Actions = {}", cmdsAll.size());
    if (statusCache.size() == 0) {
      return;
    }
    synchronized (statusCache) {
      ArrayList<ActionInfo> actionInfos = new ArrayList<>();
      for (Iterator<CmdTuple> iter = statusCache.iterator(); iter.hasNext(); ) {
        CmdTuple ct = iter.next();
        actionInfos.addAll(getActionInfoFromCommand(ct.cid));
        CommandInfo cmdinfo = cmdsAll.get(ct.cid);
        commandHashSet.remove(cmdinfo.getParameters());
        cmdsAll.remove(ct.cid);
        commandPool.deleteCommand(ct.cid);
        try {
          adapter.updateCommandStatus(ct.cid, ct.rid, ct.state);
        } catch (SQLException e) {
          LOG.error("Batch Command Status Update error!", e);
          throw new IOException(e);
        }
        iter.remove();
      }
      for (ActionInfo actionInfo : actionInfos) {
        if (actionPool.containsKey(actionInfo.getActionId())) {
          // Remove from actionPool
          actionPool.remove(actionInfo.getActionId());
        }
      }
      try {
        adapter.updateActionsTable(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
      } catch (SQLException e) {
        LOG.error("Write Cache to DB error!", e);
        throw new IOException(e);
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

  private void addToStatusCache(long cid, long rid, CommandState state) {
    statusCache.add(new CmdTuple(cid, rid, state));
  }

  private void removeFromExecuting(long cid, long rid) {
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
        addToStatusCache(cid, rid, state);
      }
      removeFromExecuting(cid, rid);
    }
  }
}
