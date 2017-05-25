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
package org.apache.hadoop.smart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.smart.actions.*;
import org.apache.hadoop.smart.mover.MoverPool;
import org.apache.hadoop.smart.sql.CommandInfo;
import org.apache.hadoop.smart.sql.DBAdapter;
import org.apache.hadoop.smart.utils.JsonUtil;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Schedule and execute commands passed down.
 */
public class CommandExecutor implements Runnable, ModuleSequenceProto {
  static final Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

  private ArrayList<Set<Long>> cmdsInState = new ArrayList<>();
  private Map<Long, CommandInfo> cmdsAll = new ConcurrentHashMap<>();
  private Set<CmdTuple> statusCache;
  private Daemon commandExecutorThread;
  private CommandPool execThreadPool;
  private DBAdapter adapter;
  private MoverPool moverPool;
  private SmartServer ssm;
  private boolean running;

  public CommandExecutor(SmartServer ssm, Configuration conf) {
    //ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    this.ssm = ssm;
    moverPool = MoverPool.getInstance();
    moverPool.init(conf);
    statusCache = new ConcurrentHashSet<>();
    for (CommandState s : CommandState.values()) {
      cmdsInState.add(s.getValue(), new HashSet<>());
    }
    execThreadPool = new CommandPool();
    running = false;
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
      MoverPool.getInstance().shutdown();
      execThreadPool.stop();
    } catch (Exception e) {
      LOG.error("Shutdown MoverPool/CommandPool Error!");
      throw new IOException(e);
    }

    // Set all thread handle to null
    execThreadPool = null;
    commandExecutorThread = null;
  }

  @Override
  public void run() {
    while (running) {
      try {
        // control the commands that executed concurrently
        if (execThreadPool == null) {
          LOG.error("Thread Init/Start Error!");
        }
        if (execThreadPool.size() <= 5) {  // TODO: use configure value
          Command toExec = schedule();
          if (toExec != null) {
            toExec.setScheduleToExecuteTime(Time.now());
            execThreadPool.execute(toExec);
          } else {
            Thread.sleep(1000);
          }
        } else {
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        if (!running)
          break;
      }
    }
  }

  public synchronized long submitCommand(CommandInfo cmd) throws IOException {
    try {
      if (adapter.insertCommandTable(cmd)) {
        cmdsAll.put(cmd.getCid(), cmd);
        cmdsInState.get(CommandState.PENDING.getValue()).add(cmd.getCid());
        return cmd.getCid();
      }
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
    return -1;
  }

  public CommandInfo getCommandInfo(long cid) throws IOException {
    if (cmdsAll.containsKey(cid))
      return cmdsAll.get(cid);
    List<CommandInfo> ret = null;
    try {
      ret = adapter.getCommandsTableItem(String.format("= %d", cid), null, null);
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
    if (ret != null)
      return ret.get(0);
    return null;
  }

  public List<CommandInfo> listCommandsInfo(long rid,
                                            CommandState commandState) throws IOException {
    List<CommandInfo> retInfos = new ArrayList<>();
    // Get from DB
    try {
      if (rid != -1)
        retInfos.addAll(adapter.getCommandsTableItem(null, String.format("= %d", rid), commandState));
      else
        retInfos.addAll(adapter.getCommandsTableItem(null, null, commandState));
    } catch (SQLException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
    // Get from Cache if commandState != CommandState.PENDING
    if (commandState != CommandState.PENDING) {
      for (Iterator<CommandInfo> iter = cmdsAll.values().iterator(); iter.hasNext(); ) {
        CommandInfo cmdinfo = iter.next();
        if (cmdinfo.getState() == commandState && cmdinfo.getRid() == rid)
          retInfos.add(cmdinfo);
      }
    }
    return retInfos;
  }

  public void activateCommand(long cid) throws IOException {
    if (inCache(cid))
      return;
    if (inUpdateCache(cid))
      return;
    CommandInfo cmdinfo = getCommandInfo(cid);
    if (cmdinfo == null || cmdinfo.getState() == CommandState.DONE)
      return;
    LOG.info("Activate Command {}", cid);
    cmdinfo.setState(CommandState.PENDING);
    addToPending(cmdinfo);
  }

  public void disableCommand(long cid) throws IOException {
    // Remove from Cache
    if (inCache(cid)) {
      LOG.info("Disable Command {}", cid);
      // Command is finished, then return
      if (inUpdateCache(cid))
        return;
      CommandInfo cmdinfo = cmdsAll.get(cid);
      cmdinfo.setState(CommandState.DISABLED);
      // Disable this command in cache
      if (inExecutingList(cid)) {
        // Remove from Executing queue
        removeFromExecuting(cid, cmdinfo.getRid(), cmdinfo.getState());
        // Kill thread
        execThreadPool.deleteCommand(cid);
      } else {
        // Remove from Pending queue
        cmdsInState.get(CommandState.PENDING.getValue()).remove(cid);
      }
      // Mark as cancelled, this status will be update to DB
      // in next batch update
      statusCache.add(new CmdTuple(cid, cmdinfo.getRid(), CommandState.DISABLED));
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
        execThreadPool.deleteCommand(cid);
      } else if (inUpdateCache(cid)) {
        RemoveFromUpdateCache(cid);
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
      throw new IOException(e);
    }
  }

  private void addToPending(CommandInfo cmdinfo) throws IOException {
    Set<Long> cmdsPending = cmdsInState.get(CommandState.PENDING.getValue());
    cmdsAll.put(cmdinfo.getCid(), cmdinfo);
    cmdsPending.add(cmdinfo.getCid());
  }

  public boolean inCache(long cid) throws IOException {
    return cmdsAll.containsKey(cid);
  }

  public boolean inExecutingList(long cid) throws IOException {
    Set<Long> cmdsExecuting = cmdsInState.get(CommandState.EXECUTING.getValue());
    return cmdsExecuting.contains(cid);
  }

  public boolean inPendingList(long cid) throws IOException {
    Set<Long> cmdsPending = cmdsInState.get(CommandState.PENDING.getValue());
    LOG.info("Size of Pending = {}", cmdsPending.size());
    return cmdsPending.contains(cid);
  }

  public boolean inUpdateCache(long cid) throws IOException {
    if (statusCache.size() == 0)
      return false;
    for (CmdTuple ct : statusCache) {
      if (ct.cid == cid)
        return true;
    }
    return false;
  }

  private void RemoveFromUpdateCache(long cid) throws IOException {
    if (statusCache.size() == 0)
      return;
    for (Iterator<CmdTuple> iter = statusCache.iterator(); iter.hasNext(); ) {
      CmdTuple ct = iter.next();
      if (ct.cid == cid) {
        iter.remove();
        break;
      }
    }
  }

  /**
   * Get command to for execution.
   * @return
   */
  private synchronized Command schedule() {
    // currently FIFO
    // List<Long> cmds = getCommands(CommandState.PENDING);
    Set<Long> cmdsPending = cmdsInState.get(CommandState.PENDING.getValue());
    Set<Long> cmdsExecuting = cmdsInState.get(CommandState.EXECUTING.getValue());
    if (cmdsPending.size() == 0) {
      // Put them into cmdsAll and cmdsInState
      if (statusCache.size() != 0)
        batchCommandStatusUpdate();
      List<CommandInfo> dbcmds = getCommandsFromDB();
      if (dbcmds == null)
        return null;
      for (CommandInfo c : dbcmds) {
        // if command alread in update cache or queue then skip
        if (cmdsAll.containsKey(c.getCid()))
          continue;
        cmdsAll.put(c.getCid(), c);
        cmdsPending.add(c.getCid());
      }
      if (cmdsPending.size() == 0)
        return null;
    }
    // TODO Update FIFO
    // Currently only get and run the first cmd
    long curr = cmdsPending.iterator().next();
    Command ret = getCommandFromCmdInfo(cmdsAll.get(curr));
    cmdsPending.remove(curr);
    cmdsExecuting.add(curr);
    ret.setState(CommandState.EXECUTING);
    return ret;
  }

  private Command getCommandFromCmdInfo(CommandInfo cmdinfo) {
    ActionBase[] actions = new ActionBase[10];
    Map<String, String> jsonParameters = JsonUtil.toStringStringMap(cmdinfo.getParameters());
    String[] args = {jsonParameters.get("_FILE_PATH_")};
    // New action
    String storagePolicy = jsonParameters.get("_STORAGE_POLICY_");
    ActionBase current;
    if (cmdinfo.getActionType().getValue() == ActionType.CacheFile.getValue()) {
      current = new MoveToCache(ssm.getDFSClient(), ssm.getConf());
    } else if (cmdinfo.getActionType().getValue() == ActionType.MoveFile.getValue()) {
      current = new MoveFile(ssm.getDFSClient(), ssm.getConf(), storagePolicy);
    } else {
      // Default Action
      current = new MoveFile(ssm.getDFSClient(), ssm.getConf(), storagePolicy);
    }
    current.initial(args);
    actions[0] = current;
    // New Command
    Command cmd = new Command(actions, new Callback());
    cmd.setParameters(jsonParameters);
    cmd.setId(cmdinfo.getCid());
    cmd.setRuleId(cmdinfo.getRid());
    cmd.setState(cmdinfo.getState());
    // Init action
    return cmd;
  }


  public List<CommandInfo> getCommandsFromDB() {
    // Get Pending cmds from DB
    try {
      return adapter.getCommandsTableItem(null, null, CommandState.PENDING);
    } catch (SQLException e) {
      // TODO: handle this issue
      LOG.error(e.getMessage());
    }
    return null;
  }

  public Long[] getCommands(CommandState state) {
    Set<Long> cmds = cmdsInState.get(state.getValue());
    return cmds.toArray(new Long[cmds.size()]);
  }

  public synchronized void batchCommandStatusUpdate() {
    LOG.info("INFO Number of Caches = {}", statusCache.size());
    LOG.info("INFO Number of Actions = {}", cmdsAll.size());
    if (statusCache.size() == 0)
      return;
    for (Iterator<CmdTuple> iter = statusCache.iterator(); iter.hasNext(); ) {
      CmdTuple ct = iter.next();
      cmdsAll.remove(ct.cid);
      try {
        adapter.updateCommandStatus(ct.cid, ct.rid, ct.state);
      } catch (SQLException e) {
        // TODO: handle this isssue
        LOG.error(e.getMessage());
      }
      iter.remove();
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
  }

  private void removeFromExecuting(long cid, long rid, CommandState state) {
    Set<Long> cmdsExecuting = cmdsInState.get(CommandState.EXECUTING.getValue());
    if (cmdsExecuting.size() == 0)
      return;
    cmdsExecuting.remove(cid);
  }

  public class Callback {

    public void complete(long cid, long rid, CommandState state) {
      commandExecutorThread.interrupt();
      // Update State in Cache
      if (cmdsAll.get(cid) == null)
        LOG.error("Command is null!");
      LOG.info("Command {} finished!", cmdsAll.get(cid));
      cmdsAll.get(cid).setState(state);
      statusCache.add(new CmdTuple(cid, rid, state));
      removeFromExecuting(cid, rid, state);
      try {
        execThreadPool.deleteCommand(cid);
      } catch (Exception e) {
        LOG.error("Shutdown Command {} Error!", cid);
      }
    }
  }
}
