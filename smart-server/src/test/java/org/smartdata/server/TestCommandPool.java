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
package org.smartdata.server;

import org.junit.Test;
import org.smartdata.server.command.Command;
import org.smartdata.server.command.CommandPool;


/**
 * CommandPool Unit Test
 */
public class TestCommandPool extends TestCommand {

  @Test
  public void addCommand() throws Exception {
    init();
    generateTestCase();
    Command cmd = runHelper();
    CommandPool cmdpoll = new CommandPool();
    cmdpoll.execute(cmd);
    cmdpoll.getCommandThread(cmd.getId()).join();
  }

  /**
   * Delete a command
   */
  @Test
  public void deleteCommand() throws Exception {
    init();
    generateTestCase();
    Command cmd = runHelper();
    CommandPool cmdpoll = new CommandPool();
    cmdpoll.execute(cmd);
    Thread.sleep(1000);
    cmdpoll.deleteCommand(cmd.getId());
  }
}
