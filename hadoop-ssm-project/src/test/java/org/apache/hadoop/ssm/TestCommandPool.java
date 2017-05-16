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

import org.apache.hadoop.ssm.CommandPool;
import org.apache.hadoop.ssm.CommandStatus;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * CommandPool Unit Test
 */
public class TestCommandPool {
  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Run a command and get status after it is finished
   */
  @Test
  public void getSingleStatus() {
    CommandPool commandPool = CommandPool.getInstance();
    UUID commandId = commandPool.runCommand("echo SSM1");
    sleep(500);
    CommandStatus commandStatus = commandPool.getCommandStatus(commandId);
    assertEquals(0, (int)commandStatus.getExitCode());
    assertEquals(true, commandStatus.isFinished());
    String[] stdOutput = commandStatus.getOutput().getStdOutput();
    assertEquals(1, stdOutput.length);
    assertEquals("SSM1", stdOutput[0]);
    assertEquals(0, commandStatus.getOutput().getStdError().length);
  }

  /**
   * Get an non-existed command
   */
  @Test
  public void getWrongCommand() {
    CommandPool commandPool = CommandPool.getInstance();
    UUID commandId = commandPool.runCommand("echo SSM1");
    UUID wrongId = UUID.nameUUIDFromBytes("This is a wrong ID".getBytes());
    assertNull(commandPool.getCommandStatus(wrongId));
  }

  /**
   * Run a command and get status multiple times while it's running
   */
  @Test
  public void getMultipleStatus() {
    CommandPool commandPool = CommandPool.getInstance();
    UUID commandId = commandPool.runCommand("echo SSM1 && sleep 3 && echo SSM2");

    // Get first time when it is still running
    sleep(1000);
    CommandStatus commandStatus = commandPool.getCommandStatus(commandId);
    assertNull(commandStatus.getExitCode());
    assertEquals(false, commandStatus.isFinished());
    String[] stdOutput = commandStatus.getOutput().getStdOutput();
    assertEquals(1, stdOutput.length);
    assertEquals("SSM1", stdOutput[0]);
    assertEquals(0, commandStatus.getOutput().getStdError().length);

    // Get second time when it is finished
    sleep(3000);
    commandStatus = commandPool.getCommandStatus(commandId);
    assertEquals(0, (int)commandStatus.getExitCode());
    assertEquals(true, commandStatus.isFinished());
    stdOutput = commandStatus.getOutput().getStdOutput();
    assertEquals(2, stdOutput.length);
    assertEquals("SSM1", stdOutput[0]);
    assertEquals("SSM2", stdOutput[1]);
    assertEquals(0, commandStatus.getOutput().getStdError().length);
  }

  @Test
  public void getStatusUntilFinished() {
    CommandPool commandPool = CommandPool.getInstance();
    UUID commandId = commandPool.runCommand("sleep 3 && echo SSM1");
    while (!commandPool.getCommandStatus(commandId).isFinished()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    CommandStatus commandStatus = commandPool.getCommandStatus(commandId);
    assertEquals(true, commandStatus.isFinished());
    String[] stdOutput = commandStatus.getOutput().getStdOutput();
    assertEquals(1, stdOutput.length);
    assertEquals("SSM1", stdOutput[0]);
    assertEquals(0, commandStatus.getOutput().getStdError().length);
  }

  /**
   * Delete a command
   */
  @Test
  public void deleteCommand() {
    CommandPool commandPool = CommandPool.getInstance();
    UUID commandId = commandPool.runCommand("echo SSM1 && sleep 3 && echo SSM2");
    sleep(1000);
    CommandStatus commandStatus = commandPool.deleteCommand(commandId);
    commandStatus = commandPool.getCommandStatus(commandId);
    assertNull(commandStatus);
  }
}
