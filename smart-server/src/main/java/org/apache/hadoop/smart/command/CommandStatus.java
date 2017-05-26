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
package org.apache.hadoop.smart.command;


/**
 * CommanStatus : the return type of GETCOMMANDSTATUS.
 */
public class CommandStatus {
  // Percentage of the execution percentage of the command, ranging from 0 to 100
  private Integer percentage;
  // Exit code for the command
  private Integer exitCode;
  // Denote whether the command has been finished, true for finished, false for unfinished
  private Boolean isFinished;
  // Output of the command
  private OutPutType output;

  class OutPutType {
    private String[] stdOutput;
    private String[] stdError;

    public OutPutType() {
      stdOutput = new String[0];
      stdError = new String[0];
    }

    public String[] getStdOutput() {
      return stdOutput;
    }

    public String[] getStdError() {
      return stdError;
    }

    public void setStdOutput(String[] stdOutput) {
      this.stdOutput = stdOutput;
    }

    public void setStdError(String[] stdError) {
      this.stdError = stdError;
    }
  }

  public CommandStatus() {
    percentage = null;
    exitCode = null;
    isFinished = null;
    output = new OutPutType();
  }

  public Double getPercentage() {
    return percentage/100.0;
  }

  public Integer getExitCode() {
    return exitCode;
  }

  public Boolean isFinished() {
    return isFinished;
  }

  public OutPutType getOutput() {
    return output;
  }

  public void setPercentage(Integer percentage) {
    this.percentage = percentage;
  }

  public void setExitCode(Integer exitCode) {
    this.exitCode = exitCode;
  }

  public void setIsFinished(Boolean isFinished) {
    this.isFinished = isFinished;
  }

  public void setOutput(String[] stdOutput, String[] stdError) {
    output.setStdOutput(stdOutput);
    output.setStdError(stdError);
  }
}
