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
package org.smartdata.common.protocol;

import org.smartdata.actions.ActionDescriptor;
import org.smartdata.common.CommandState;
import org.smartdata.common.SmartServiceState;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.common.command.CommandInfo;

import java.io.IOException;
import java.util.List;

public interface SmartAdminProtocol {

  SmartServiceState getServiceState() throws IOException;

  long submitRule(String rule, RuleState initState) throws IOException;

  /**
   * Check if it is a valid rule.
   * @param rule
   * @throws IOException if not valid
   */
  void checkRule(String rule) throws IOException;

  /**
   * Get information about the given rule.
   * @param ruleID
   * @return
   * @throws IOException
   */
  RuleInfo getRuleInfo(long ruleID) throws IOException;

  /**
   * List rules in SSM.
   * TODO: maybe we can return only info about rules in certain state.
   * @return
   * @throws IOException
   */
  List<RuleInfo> listRulesInfo() throws IOException;

  /**
   * Delete a rule in SSM. if dropPendingCommands equals false then the rule
   * record will still be kept in Table 'rules', the record will be deleted
   * sometime later.
   *
   * @param ruleID
   * @param dropPendingCommands pending commands triggered by the rule will be
   *                            discarded if true.
   * @throws IOException
   */
  void deleteRule(long ruleID, boolean dropPendingCommands) throws IOException;

  void activateRule(long ruleID) throws IOException;

  void disableRule(long ruleID, boolean dropPendingCommands) throws IOException;

  /**
   * Get information about the given command.
   * @param commandID
   * @return CommandInfo
   * @throws IOException
   */
  CommandInfo getCommandInfo(long commandID) throws IOException;

  /**
   * List commands in SSM.
   * @param ruleID
   * @param commandState
   * @return All List<CommandInfo> commandInfos that satisfy requirement
   * @throws IOException
   */
  List<CommandInfo> listCommandInfo(long ruleID, CommandState commandState) throws IOException;

  /**
   * Get information about the given command.
   * @param commandID
   * @return CommandInfo
   * @throws IOException
   */
  void activateCommand(long commandID) throws IOException;

  /**
   * Disable Command, if command is PENDING then mark as DISABLE
   * if command is EXECUTING then kill all actions unfinished
   * then mark as DISABLE, if command is DONE then do nothing.
   * @param commandID
   * @throws IOException
   */
  void disableCommand(long commandID) throws IOException;

  /**
   * Delete Command from DB and Cache. If command is in Cache,
   * then disable it.
   * @param commandID
   * @throws IOException
   */
  void deleteCommand(long commandID) throws IOException;

  ActionInfo getActionInfo(long actionID) throws IOException;

  List<ActionInfo> listActionInfoOfLastActions(int maxNumActions)
      throws IOException;

  long submitCommand(String cmd) throws IOException;

  List<ActionDescriptor> listActionsSupported() throws IOException;

}
