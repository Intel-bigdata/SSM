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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * CommandPool : A singleton class to manage all commandsThread
 */
public class CommandPool {
  static final Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

  private Map<Long, Command> commandMap;
  private Map<Long, Thread> commandThread;

  CommandPool() {
    commandMap = new ConcurrentHashMap<>();
    commandThread = new ConcurrentHashMap<>();
  }

  public int size() {
    return commandMap.size();
  }

  public void stop() throws Exception {
    for (Long cid : commandMap.keySet()) {
      deleteCommand(cid);
    }
  }

  // Delete a command from the pool
  public void deleteCommand(long cid) throws IOException {
    if (!commandMap.containsKey(cid)) {
      return;
    }
    Command cmd = commandMap.get(cid);
    if (cmd != null) {
      LOG.error("Force Terminate Command {}", cmd.toString());
      cmd.stop();
    }
    commandMap.remove(cid);
    commandThread.remove(cid);
  }

  public Command getCommand(long cid) {
    return commandMap.get(cid);
  }

  public Thread getCommandThread(long cid) {
    return commandThread.get(cid);
  }

  public void execute(Command cmd) {
    commandMap.put(cmd.getId(), cmd);
    Thread cthread = new Thread(cmd);
    commandThread.put(cmd.getId(), cthread);
    cthread.start();
  }
}
