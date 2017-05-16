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
package org.apache.hadoop.ssm;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ssm.actions.*;
import org.apache.hadoop.ssm.sql.CommandInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.utils.JsonUtil;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.*;

/**
 * Schedule and execute commands passed down.
 */
public class CommandExecutor implements Runnable, ModuleSequenceProto {
  private ArrayList<Set<Long>> cmdsInState = new ArrayList<>();
  private Map<Long, Command> cmdsAll = new HashMap<>();
  private ArrayList<CmdTuple> statusCache = new ArrayList<>();
  private Daemon commandExecutorThread;
  private ThreadGroup execThreadGroup;
  private DBAdapter adapter;

  private SSMServer ssm;

  public CommandExecutor(SSMServer ssm, Configuration conf) {
    //ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    this.ssm = ssm;
    for (CommandState s : CommandState.values()) {
      cmdsInState.add(s.getValue(), new HashSet<Long>());
    }
    execThreadGroup = new ThreadGroup("CommandExecutorWorker");
    execThreadGroup.setDaemon(true);
  }

  public boolean init(DBAdapter adapter) throws IOException {
    if(adapter != null) {
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
    return true;
  }

  /**
   * Stop CommandExecutor
   */
  public void stop() throws IOException {
    // TODO Command statue update

    if (commandExecutorThread == null) {
      return;
    }

    commandExecutorThread.interrupt();
    execThreadGroup.interrupt();
    join();
    execThreadGroup.destroy();
    try {
      commandExecutorThread.join(1000);
    } catch (InterruptedException e) {
    }
    commandExecutorThread = null;
    execThreadGroup = null;
  }

  public void join() throws IOException {
    try{
      Thread[] gThread = new Thread[execThreadGroup.activeCount()];
      execThreadGroup.enumerate(gThread);
      for(Thread t: gThread)
        t.join();
    } catch (InterruptedException e) {
      System.out.printf("Stop thread group error!");
    }
  }

  @Override
  public void run() {
    boolean running = true;
    while (running) {
      try {
        // control the commands that executed concurrently
        if (execThreadGroup.activeCount() <= 5) {  // TODO: use configure value
          Command toExec = schedule();
          if (toExec != null) {
            toExec.setScheduleToExecuteTime(Time.now());
//            cmdsInState.get(CommandState.PENDING.getValue())
//                .add(toExec.getId());
            new Daemon(execThreadGroup, toExec).start();
          } else {
            Thread.sleep(1000);
          }
        } else {
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        running = false;
      }
    }
  }

  /**
   * Add a command to CommandExecutor for execution.
   * @param cmd
   */
  public synchronized void addCommand(Command cmd) {
    cmdsAll.put(cmd.getId(), cmd);
  }

  public Command getCommand(long id) {
    return cmdsAll.get(id);
  }

  /**
   * Get command to for execution.
   * @return
   */
  private synchronized Command schedule() {
    // currently FIFO
    // List<Long> cmds = getCommands(CommandState.PENDING);
    Set<Long> cmds = cmdsInState.get(CommandState.PENDING.getValue());
    Set<Long> cmdsExecuting = cmdsInState.get(CommandState.EXECUTING.getValue());
    if (cmds.size() == 0) {
      // TODO Check Status and Update
      // Put them into cmdsAll and cmdsInState
      if(statusCache.size() != 0)
        batchCommandStatusUpdate();
      List<CommandInfo> dbcmds = getCommansFromDB();
      for(CommandInfo c : dbcmds) {
        if(cmdsExecuting.contains(c.getCid()))
          continue;
        Command cmd = getCommand(c, ssm);
        cmdsAll.put(cmd.getId(), cmd);
        cmds.add(cmd.getId());
      }
      if (cmds.size() == 0)
        return null;
    }

    // TODO Update FIFO
    // Currently only get and run the first cmd
    long curr = cmds.iterator().next();
    Command ret = cmdsAll.get(curr);
    cmdsAll.remove(curr);
    cmds.remove(curr);
    cmdsExecuting.add(curr);
    return ret;
  }

  public Command getCommand(CommandInfo cmdinfo, SSMServer ssm) {
    ActionBase[] actions = new ActionBase[10];
    Map<String, String> jsonParameters = JsonUtil.toStringStringMap(cmdinfo.getParameters());
    String[] args = {jsonParameters.get("_FILE_PATH_")};
    // New action
    String storagePolicy = jsonParameters.get("_STORAGE_POLICY_");
    ActionBase current;
    if(cmdinfo.getActionType().getValue() == ActionType.CacheFile.getValue()) {
      current = new MoveToCache(ssm.getDFSClient(), ssm.getConf());
    } else if(cmdinfo.getActionType().getValue()  == ActionType.MoveFile.getValue()) {
      current = new MoveFile(ssm.getDFSClient(), ssm.getConf(), storagePolicy);
    } else {
      // TODO Default
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


  public List<CommandInfo> getCommansFromDB() {
    // Get Pending cmds from DB
    return adapter.getCommandsTableItem(null, null, CommandState.PENDING);
  }

  public Long[] getCommands(CommandState state) {
    Set<Long> cmds = cmdsInState.get(state.getValue());
    return cmds.toArray(new Long[cmds.size()]);
  }

  public synchronized void batchCommandStatusUpdate() {
    if(statusCache.size() == 0)
      return;
    for(CmdTuple ct: statusCache)
      adapter.updateCommandStatus(ct.cid, ct.rid, ct.state);
    statusCache.clear();
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

  public void removeFromExecuting(long cid, long rid, CommandState state) {
    Set<Long> cmdsExecuting = cmdsInState.get(CommandState.EXECUTING.getValue());
    if(cmdsExecuting.size() == 0)
      return;
    cmdsExecuting.remove(cid);
  }

  public class Callback {
    public void complete(long cid, long rid, CommandState state) {
//      adapter.updateCommandStatus(cid, rid, state);
        statusCache.add(new CmdTuple(cid, rid, state));
        removeFromExecuting(cid, rid, state);
    }
  }
}
