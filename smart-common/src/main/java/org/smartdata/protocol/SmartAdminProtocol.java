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
package org.smartdata.protocol;

import org.apache.hadoop.security.KerberosInfo;
import org.smartdata.SmartServiceState;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.ActionDescriptor;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;

import java.io.IOException;
import java.util.List;

@KerberosInfo(
  serverPrincipal = SmartConfKeys.SMART_SERVER_KERBEROS_PRINCIPAL_KEY)
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
   * Delete a rule in SSM. if dropPendingCmdlets equals false then the rule
   * record will still be kept in Table 'rules', the record will be deleted
   * sometime later.
   *
   * @param ruleID
   * @param dropPendingCmdlets pending cmdlets triggered by the rule will be
   *                            discarded if true.
   * @throws IOException
   */
  void deleteRule(long ruleID, boolean dropPendingCmdlets) throws IOException;

  void activateRule(long ruleID) throws IOException;

  void disableRule(long ruleID, boolean dropPendingCmdlets) throws IOException;

  /**
   * Get information about the given cmdlet.
   * @param cmdletID
   * @return CmdletInfo
   * @throws IOException
   */
  CmdletInfo getCmdletInfo(long cmdletID) throws IOException;

  /**
   * List cmdlets in SSM.
   * @param ruleID
   * @param cmdletState
   * @return All cmdletInfos that satisfy requirement
   * @throws IOException
   */
  List<CmdletInfo> listCmdletInfo(long ruleID, CmdletState cmdletState) throws IOException;

  /**
   * Get information about the given cmdlet.
   * @param cmdletID
   * @return CmdletInfo
   * @throws IOException
   */
  void activateCmdlet(long cmdletID) throws IOException;

  /**
   * Disable Cmdlet, if cmdlet is PENDING then mark as DISABLE
   * if cmdlet is EXECUTING then kill all actions unfinished
   * then mark as DISABLE, if cmdlet is DONE then do nothing.
   * @param cmdletID
   * @throws IOException
   */
  void disableCmdlet(long cmdletID) throws IOException;

  /**
   * Delete Cmdlet from DB and Cache. If cmdlet is in Cache,
   * then disable it.
   * @param cmdletID
   * @throws IOException
   */
  void deleteCmdlet(long cmdletID) throws IOException;

  /**
   * Query action info using action id.
   * @param actionID
   * @return
   * @throws IOException
   */
  ActionInfo getActionInfo(long actionID) throws IOException;

  /**
   * Return info of actions newly generated.
   * @param maxNumActions maximum number of actions
   * @return
   * @throws IOException
   */
  List<ActionInfo> listActionInfoOfLastActions(int maxNumActions)
      throws IOException;

  /**
   * Submit a cmdlet to server.
   * @param cmd separate actions with ';'
   * @return cmdlet id if success
   * @throws IOException
   */
  long submitCmdlet(String cmd) throws IOException;

  /**
   * List actions supported in SmartServer.
   * @return
   * @throws IOException
   */
  List<ActionDescriptor> listActionsSupported() throws IOException;

}
