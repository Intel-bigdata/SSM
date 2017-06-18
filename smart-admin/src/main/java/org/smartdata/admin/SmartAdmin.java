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
package org.smartdata.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.smartdata.admin.protocolPB.SmartAdminProtocolAdminSideTranslatorPB;
import org.smartdata.common.CommandState;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.security.JaasLoginUtil;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.common.SmartServiceState;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.common.protocol.SmartAdminProtocol;
import org.smartdata.common.protocolPB.SmartAdminProtocolPB;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class SmartAdmin implements java.io.Closeable, SmartAdminProtocol {
  final static long VERSION = 1;
  Configuration conf;
  SmartAdminProtocol ssm;
  volatile boolean clientRunning = true;

  public SmartAdmin(Configuration conf)
      throws IOException {
    this.conf = conf;
    loginAsSmartAdmin();
    String[] strings = conf.get(SmartConfKeys.DFS_SSM_RPC_ADDRESS_KEY,
        SmartConfKeys.DFS_SSM_RPC_ADDRESS_DEFAULT).split(":");
    InetSocketAddress address = new InetSocketAddress(
        strings[strings.length - 2],
        Integer.parseInt(strings[strings.length - 1]));
    RPC.setProtocolEngine(conf, SmartAdminProtocolPB.class,
        ProtobufRpcEngine.class);
    SmartAdminProtocolPB proxy = RPC.getProxy(
        SmartAdminProtocolPB.class, VERSION, address, conf);
    this.ssm = new SmartAdminProtocolAdminSideTranslatorPB(proxy);
  }

  private boolean isSecurityEnabled() {
    return conf.getBoolean(SmartConfKeys.DFS_SSM_SECURITY_ENABLE, false);
  }

  public void loginAsSmartAdmin() throws IOException {
    if (!isSecurityEnabled()) {
      return;
    }
    String principal = conf.get(SmartConfKeys.DFS_SSM_KERBEROS_PRINCIPAL_KEY,
        System.getProperty("user.name"));
    JaasLoginUtil.loginUsingTicketCache(principal);
  }

  @Override
  public void close() {
    if (clientRunning) {
      RPC.stopProxy(ssm);
      ssm = null;
      this.clientRunning = false;
    }
  }

  public void checkOpen() throws IOException {
    if (!clientRunning) {
      throw new IOException("SmartAdmin closed");
    }
  }

  public SmartServiceState getServiceState() throws IOException {
    checkOpen();
    return ssm.getServiceState();
  }

  public void checkRule(String rule) throws IOException {
    checkOpen();
    ssm.checkRule(rule);
  }

  public long submitRule(String rule, RuleState initState)
      throws IOException {
    checkOpen();
    return ssm.submitRule(rule, initState);
  }

  public RuleInfo getRuleInfo(long id) throws IOException {
    checkOpen();
    return ssm.getRuleInfo(id);
  }

  public List<RuleInfo> listRulesInfo() throws IOException {
    checkOpen();
    return ssm.listRulesInfo();
  }

  @Override
  public void deleteRule(long ruleID, boolean dropPendingCommands)
      throws IOException {
    checkOpen();
    ssm.deleteRule(ruleID, dropPendingCommands);
  }

  @Override
  public void activateRule(long ruleID) throws IOException {
    checkOpen();
    ssm.activateRule(ruleID);
  }

  @Override
  public void disableRule(long ruleID, boolean dropPendingCommands)
      throws IOException {
    checkOpen();
    ssm.disableRule(ruleID, dropPendingCommands);
  }

  @Override
  public CommandInfo getCommandInfo(long commandID) throws IOException {
    checkOpen();
    return ssm.getCommandInfo(commandID);
  }

  @Override
  public List<CommandInfo> listCommandInfo(long rid, CommandState commandState)
      throws IOException {
    checkOpen();
    return ssm.listCommandInfo(rid, commandState);
  }

  @Override
  public void activateCommand(long commandID) throws IOException {
    checkOpen();
    ssm.activateCommand(commandID);
  }

  @Override
  public void disableCommand(long commandID) throws IOException {
    checkOpen();
    ssm.disableCommand(commandID);
  }

  @Override
  public void deleteCommand(long commandID) throws IOException {
    checkOpen();
    ssm.deleteCommand(commandID);
  }

  @Override
  public ActionInfo getActionInfo(long actionID) throws IOException {
    checkOpen();
    return ssm.getActionInfo(actionID);
  }

  @Override
  public List<ActionInfo> listActionInfoOfLastActions(int maxNumActions) throws IOException {
    checkOpen();
    return ssm.listActionInfoOfLastActions(maxNumActions);
  }

  @Override
  public long submitCommand(String cmd) throws IOException {
    checkOpen();
    return ssm.submitCommand(cmd);
  }

  @Override
  public List<ActionDescriptor> listActionsSupported() throws IOException {
    checkOpen();
    return ssm.listActionsSupported();
  }
}