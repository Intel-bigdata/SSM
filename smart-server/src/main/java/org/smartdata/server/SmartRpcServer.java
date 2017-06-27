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

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RetriableException;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.CmdletState;
import org.smartdata.common.models.ActionInfo;
import org.smartdata.common.protocol.ClientServerProto;
import org.smartdata.common.protocol.SmartServerProtocols;
import org.smartdata.common.protocolPB.SmartClientProtocolPB;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.common.SmartServiceState;
import org.smartdata.common.protocol.AdminServerProto;
import org.smartdata.common.protocolPB.SmartAdminProtocolPB;
import org.smartdata.common.protocolPB.ClientSmartProtocolServerSideTranslatorPB;
import org.smartdata.common.models.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.common.models.CmdletInfo;
import org.smartdata.metrics.FileAccessEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the rpc calls.
 * TODO: Implement statistics for SSM rpc server
 */
public class SmartRpcServer implements SmartServerProtocols {
  protected SmartServer ssm;
  protected Configuration conf;
  protected final InetSocketAddress clientRpcAddress;
  protected int serviceHandlerCount = 1;
  protected final RPC.Server clientRpcServer;

  public SmartRpcServer(SmartServer ssm, Configuration conf) throws IOException {
    this.ssm = ssm;
    this.conf = conf;
    // TODO: implement ssm SmartAdminProtocol
    InetSocketAddress rpcAddr = getRpcServerAddress();
    RPC.setProtocolEngine(conf, SmartAdminProtocolPB.class, ProtobufRpcEngine.class);

    ClientSmartProtocolServerSideTranslatorPB clientSSMProtocolServerSideTranslatorPB
        = new ClientSmartProtocolServerSideTranslatorPB(this);

    BlockingService adminSmartPbService = AdminServerProto.protoService
        .newReflectiveBlockingService(clientSSMProtocolServerSideTranslatorPB);
    BlockingService clientSmartPbService = ClientServerProto.protoService
        .newReflectiveBlockingService(clientSSMProtocolServerSideTranslatorPB);

    // TODO: provide service for SmartClientProtocol and SmartAdminProtocol
    // TODO: in different port and server
    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(SmartAdminProtocolPB.class)
        .setInstance(adminSmartPbService)
        .setBindAddress(rpcAddr.getHostName())
        .setPort(rpcAddr.getPort())
        .setNumHandlers(serviceHandlerCount)
        .setVerbose(true)
        .build();

    InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
    clientRpcAddress = new InetSocketAddress(
        rpcAddr.getHostName(), listenAddr.getPort());

    DFSUtil.addPBProtocol(conf, SmartAdminProtocolPB.class,
        adminSmartPbService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, SmartClientProtocolPB.class,
        clientSmartPbService, clientRpcServer);
  }

  private InetSocketAddress getRpcServerAddress() {
    String[] strings = conf.get(SmartConfKeys.DFS_SSM_RPC_ADDRESS_KEY,
        SmartConfKeys.DFS_SSM_RPC_ADDRESS_DEFAULT).split(":");
    return new InetSocketAddress(strings[strings.length - 2]
        , Integer.parseInt(strings[strings.length - 1]));
  }

  /**
   * Start SSM RPC service
   */
  public void start() {
    // TODO: start clientRpcServer
    if (clientRpcServer != null) {
      clientRpcServer.start();
    }
  }

  /**
   * Stop SSM RPC service
   */
  public void stop() {
    if (clientRpcServer != null) {
      clientRpcServer.stop();
    }
  }

  /*
   * Waiting for RPC threads to exit.
   */
  public void join() throws InterruptedException {
    if (clientRpcServer != null) {
      clientRpcServer.join();
    }
  }

  @Override
  public SmartServiceState getServiceState() {
    return ssm.getSSMServiceState();
  }

  private void checkIfActive() throws IOException {
    if (!ssm.isActive()) {
      throw new RetriableException("SSM services not ready...");
    }
  }

  @Override
  public long submitRule(String rule, RuleState initState) throws IOException {
    checkIfActive();
    return ssm.getRuleManager().submitRule(rule, initState);
  }

  @Override
  public void checkRule(String rule) throws IOException {
    checkIfActive();
    ssm.getRuleManager().checkRule(rule);
  }

  @Override
  public RuleInfo getRuleInfo(long ruleId) throws IOException {
    checkIfActive();
    return ssm.getRuleManager().getRuleInfo(ruleId);
  }

  @Override
  public List<RuleInfo> listRulesInfo() throws IOException {
    checkIfActive();
    return ssm.getRuleManager().listRulesInfo();
  }

  @Override
  public void deleteRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    checkIfActive();
    ssm.getRuleManager().deleteRule(ruleID, dropPendingCmdlets);
  }

  @Override
  public void activateRule(long ruleID) throws IOException {
    checkIfActive();
    ssm.getRuleManager().activateRule(ruleID);
  }

  @Override
  public void disableRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    checkIfActive();
    ssm.getRuleManager().disableRule(ruleID, dropPendingCmdlets);
  }

  @Override
  public CmdletInfo getCmdletInfo(long cmdletID) throws IOException {
    checkIfActive();
    return ssm.getCmdletManager().getCmdletInfo(cmdletID);
  }

  @Override
  public List<CmdletInfo> listCmdletInfo(long rid, CmdletState cmdletState)
      throws IOException {
    checkIfActive();
    return ssm.getCmdletManager().listCmdletsInfo(rid, cmdletState);
  }

  @Override
  public void activateCmdlet(long cmdletID) throws IOException {
    checkIfActive();
    ssm.getCmdletManager().activateCmdlet(cmdletID);
  }

  @Override
  public void disableCmdlet(long cmdletID) throws IOException {
    checkIfActive();
    ssm.getCmdletManager().disableCmdlet(cmdletID);
  }

  @Override
  public void deleteCmdlet(long cmdletID) throws IOException {
    checkIfActive();
    ssm.getCmdletManager().deleteCmdlet(cmdletID);
  }

  @Override
  public ActionInfo getActionInfo(long actionID) throws IOException {
    checkIfActive();
    return ssm.getCmdletManager().getActionInfo(actionID);
  }

  @Override
  public List<ActionInfo> listActionInfoOfLastActions(int maxNumActions)
      throws IOException {
    checkIfActive();
    return ssm.getCmdletManager().listNewCreatedActions(maxNumActions);
  }

  @Override
  public void reportFileAccessEvent(FileAccessEvent event) throws IOException {
    checkIfActive();
    ssm.getStatesManager().reportFileAccessEvent(event);
  }

  @Override
  public long submitCmdlet(String cmd) throws IOException {
    checkIfActive();
    // TODO: to be implemented
    return ssm.getCmdletManager().submitCmdlet(cmd);
  }

  @Override
  public List<ActionDescriptor> listActionsSupported() throws IOException {
    checkIfActive();
    // TODO: to be implemented
    return new ArrayList<>();
  }
}
