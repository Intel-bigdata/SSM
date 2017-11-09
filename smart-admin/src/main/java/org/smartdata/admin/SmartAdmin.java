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
import org.smartdata.SmartServiceState;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.ActionDescriptor;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.protocol.SmartAdminProtocol;
import org.smartdata.protocol.protobuffer.AdminProtocolClientSideTranslator;
import org.smartdata.protocol.protobuffer.AdminProtocolProtoBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class SmartAdmin implements java.io.Closeable, SmartAdminProtocol {
  static final long VERSION = 1;
  Configuration conf;
  SmartAdminProtocol ssm;
  volatile boolean clientRunning = true;

  public SmartAdmin(Configuration conf)
      throws IOException {
    this.conf = conf;
    String[] strings = conf.get(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY,
        SmartConfKeys.SMART_SERVER_RPC_ADDRESS_DEFAULT).split(":");
    InetSocketAddress address = new InetSocketAddress(
        strings[strings.length - 2],
        Integer.parseInt(strings[strings.length - 1]));
    RPC.setProtocolEngine(conf, AdminProtocolProtoBuffer.class,
        ProtobufRpcEngine.class);
    AdminProtocolProtoBuffer proxy = RPC.getProxy(
        AdminProtocolProtoBuffer.class, VERSION, address, conf);
    this.ssm = new AdminProtocolClientSideTranslator(proxy);
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
  public void deleteRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    checkOpen();
    ssm.deleteRule(ruleID, dropPendingCmdlets);
  }

  @Override
  public void activateRule(long ruleID) throws IOException {
    checkOpen();
    ssm.activateRule(ruleID);
  }

  @Override
  public void disableRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    checkOpen();
    ssm.disableRule(ruleID, dropPendingCmdlets);
  }

  @Override
  public CmdletInfo getCmdletInfo(long cmdletID) throws IOException {
    checkOpen();
    return ssm.getCmdletInfo(cmdletID);
  }

  @Override
  public List<CmdletInfo> listCmdletInfo(long rid, CmdletState cmdletState)
      throws IOException {
    checkOpen();
    return ssm.listCmdletInfo(rid, cmdletState);
  }

  @Override
  public void activateCmdlet(long cmdletID) throws IOException {
    checkOpen();
    ssm.activateCmdlet(cmdletID);
  }

  @Override
  public void disableCmdlet(long cmdletID) throws IOException {
    checkOpen();
    ssm.disableCmdlet(cmdletID);
  }

  @Override
  public void deleteCmdlet(long cmdletID) throws IOException {
    checkOpen();
    ssm.deleteCmdlet(cmdletID);
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
  public long submitCmdlet(String cmd) throws IOException {
    checkOpen();
    return ssm.submitCmdlet(cmd);
  }

  @Override
  public List<ActionDescriptor> listActionsSupported() throws IOException {
    checkOpen();
    return ssm.listActionsSupported();
  }
}
