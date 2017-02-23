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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * CommandPool : A singleton class to manage all commands
 */
public class CommandPool {
  private static CommandPool instance = new CommandPool();
  private Map<UUID, Command> commandMap;

  private CommandPool() {
    commandMap = new ConcurrentHashMap<>();
  }

  public static CommandPool getInstance() {
    return instance;
  }

  // Add a command and return its ID
  public UUID runCommand(String command) {
    Long currentTime = System.currentTimeMillis();
    String commandWithTime = command + currentTime.toString();
    UUID commandId = UUID.nameUUIDFromBytes(commandWithTime.getBytes());
    Command commandEntity = new Command(command);
    commandMap.put(commandId, commandEntity);
    ExecutorService exec = Executors.newCachedThreadPool();
    exec.execute(commandEntity);
    return commandId;
  }

  // Get the status of a command by its ID
  public CommandStatus getCommandStatus(UUID commandId) {
    Command commandEntity = commandMap.get(commandId);
    if (commandEntity == null) {
      return null;
    }
    CommandStatus commandStatus = new CommandStatus();
    synchronized (commandEntity) {
      commandStatus.setExitCode(commandEntity.getExitCode());
      commandStatus.setIsFinished(commandEntity.isFinished());
      commandStatus.setOutput(commandEntity.getOutput(), commandEntity.getError());
    }
    return commandStatus;
  }

  // Delete a command from the pool
  public CommandStatus deleteCommand(UUID commandId) {
    Command commandEntity = commandMap.get(commandId);
    if (commandEntity == null) {
      return null;
    }
    CommandStatus commandStatus = new CommandStatus();
    commandEntity.kill();
    synchronized (commandEntity) {
      commandStatus.setExitCode(commandEntity.getExitCode());
      commandStatus.setIsFinished(commandEntity.isFinished());
      commandStatus.setOutput(commandEntity.getOutput(), commandEntity.getError());
    }
    commandMap.remove(commandId);
    return commandStatus;
  }

  /**
   * Save stdout/stderr of a command
   */
  class StreamHandler implements Runnable {
    private InputStream is;
    private String type;
    private LinkedBlockingQueue<String> lines;

    public StreamHandler(InputStream is, String type, LinkedBlockingQueue<String> lines) {
      this.is = is;
      this.type = type;
      this.lines = lines;
    }

    public String getType() {
      return type;
    }

    public void run() {
      try {
        BufferedReader input = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = input.readLine()) != null) {
          lines.put(line);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Run a command and manage all its information
   */
  class Command implements Runnable{
    private String command;
    private LinkedBlockingQueue<String> stdOutput;
    private LinkedBlockingQueue<String> stdError;
    private Process process;
    volatile private Boolean isFinished;
    volatile private Integer exitCode;

    public Command(String command) {
      this.command = command;
      process = null;
      stdOutput = new LinkedBlockingQueue<String>();
      stdError = new LinkedBlockingQueue<String>();
      isFinished = false;
      exitCode = null;
    }

    public String[] getOutput() {
      String[] stdOutputArray = stdOutput.toArray(new String[0]);
      //stdOutput.clear();
      return stdOutputArray;
    }

    public String[] getError() {
      String[] stdErrorArray = stdError.toArray(new String[0]);
      //stdError.clear();
      return stdErrorArray;
    }

    public String getCommand() {
      return command;
    }

    public Boolean isFinished() {
      return isFinished;
    }

    public Integer getExitCode() {
      return exitCode;
    }

    public void run() {
      try {
        // Start a process to run the command
        List<String> cmds = new ArrayList<>();
        cmds.add("sh");
        cmds.add("-c");
        cmds.add(command);
        ProcessBuilder processBuilder = new ProcessBuilder(cmds);
        process = processBuilder.start();

        // Get stdout and stderr stream
        StreamHandler outputStream = new StreamHandler(process.getInputStream(), "OUTPUT", stdOutput);
        StreamHandler errorStream = new StreamHandler(process.getErrorStream(), "ERROR", stdError);
        ExecutorService exec = Executors.newCachedThreadPool();
        exec.execute(outputStream);
        exec.execute(errorStream);

        // Wait for process and get exitcode
        exitCode = process.waitFor();
        isFinished = true;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void kill() {
      if (process.isAlive()) {
        process.destroy();
      }
    }
  }



}
