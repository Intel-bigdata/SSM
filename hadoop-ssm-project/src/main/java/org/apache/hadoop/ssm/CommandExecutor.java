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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Schedule and execute commands passed down.
 */
public class CommandExecutor implements Runnable, ModuleSequenceProto {
  private ArrayList<List<Long>> cmdsInState = new ArrayList<>();
  private Map<Long, Command> cmdsAll = new HashMap<>();

  private Daemon commandExecutorThread;
  private ThreadGroup execThreadGroup;

  private SSMServer ssm;

  public CommandExecutor(SSMServer ssm, Configuration conf) {
    //ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    this.ssm = ssm;
    for (CommandState s : CommandState.values()) {
      cmdsInState.add(s.getValue(), new LinkedList<>());
    }
    execThreadGroup = new ThreadGroup("CommandExecutorWorker");
    execThreadGroup.setDaemon(true);
  }

  public boolean init(DBAdapter adapter) throws IOException {
    return true;
  }

  /**
   * Start CommandExecutor.
   */
  public boolean start() throws IOException {
    commandExecutorThread = new Daemon(this);
    commandExecutorThread.setName(this.getClass().getCanonicalName());
    commandExecutorThread.start();
    return true;
  }

  /**
   * Stop CommandExecutor
   */
  public void stop() throws IOException {
    if (commandExecutorThread == null) {
      return;
    }

    commandExecutorThread.interrupt();
    execThreadGroup.interrupt();
    execThreadGroup.destroy();
    try {
      commandExecutorThread.join(1000);
    } catch (InterruptedException e) {
    }
    commandExecutorThread = null;
    execThreadGroup = null;
  }

  public void join() throws IOException {
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
            cmdsInState.get(CommandState.PENDING.getValue())
                .add(toExec.getId());
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
    List<Long> cmds = cmdsInState.get(CommandState.PENDING.getValue());
    if (cmds.size() == 0) {
      return null;
    }
    Command ret = cmdsAll.get(cmds.get(0));
    cmds.remove(0);
    return ret;
  }

  public Long[] getCommands(CommandState state) {
    List<Long> cmds = cmdsInState.get(state.getValue());
    return cmds.toArray(new Long[cmds.size()]);
  }
}
