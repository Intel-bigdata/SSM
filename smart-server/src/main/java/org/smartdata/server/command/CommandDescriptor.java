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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class CommandDescriptor {
  private long ruleId;
  private List<String> actionNames = new ArrayList<>();
  private List<String[]> actionArgs = new ArrayList<>();

  public CommandDescriptor() {
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public void addAction(String actionName, String[] args) {
    actionNames.add(actionName);
    actionArgs.add(args);
  }

  public String getActionName(int index) {
    return actionNames.get(index);
  }

  public String[] getActionArgs(int index) {
    return actionArgs.get(index);
  }

  public int getNumActions() {
    return actionNames.size();
  }

  // TODO: to be implemented
  /**
   * Construct an CommandDescriptor from command string.
   * @param cmdString
   * @return
   * @throws ParseException
   */
  public static CommandDescriptor fromCommandString(String cmdString)
      throws ParseException {
    return new CommandDescriptor();
  }
}
