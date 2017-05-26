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
import org.smartdata.common.CommandState;
import org.smartdata.common.SmartConfigureKeys;
import org.smartdata.common.SmartServiceState;
import org.smartdata.common.protocol.SmartAdminProtocol;
import org.smartdata.common.protocol.AdminServerProto;
import org.smartdata.common.protocolPB.SmartAdminProtocolPB;
import org.smartdata.common.protocolPB.ClientSmartProtocolServerSideTranslatorPB;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.common.command.CommandInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Implements the rpc calls.
 * TODO: Implement statistics for SSM rpc server
 */
public class SmartRpcServer implements SmartAdminProtocol {
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

    BlockingService clientSmartPbService = AdminServerProto.protoService
        .newReflectiveBlockingService(clientSSMProtocolServerSideTranslatorPB);

    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(SmartAdminProtocolPB.class)
        .setInstance(clientSmartPbService)
        .setBindAddress(rpcAddr.getHostName())
        .setPort(rpcAddr.getPort())
        .setNumHandlers(serviceHandlerCount)
        .setVerbose(true)
        .build();

    InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
    clientRpcAddress = new InetSocketAddress(
        rpcAddr.getHostName(), listenAddr.getPort());

    DFSUtil.addPBProtocol(conf, SmartAdminProtocolPB.class,
        clientSmartPbService, clientRpcServer);
  }

  private InetSocketAddress getRpcServerAddress() {
    String[] strings = conf.get(SmartConfigureKeys.DFS_SSM_RPC_ADDRESS_KEY,
        SmartConfigureKeys.DFS_SSM_RPC_ADDRESS_DEFAULT).split(":");
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
  public void deleteRule(long ruleID, boolean dropPendingCommands) throws IOException {
    checkIfActive();
    ssm.getRuleManager().DeleteRule(ruleID, dropPendingCommands);
  }

  @Override
  public void activateRule(long ruleID) throws IOException {
    checkIfActive();
    ssm.getRuleManager().ActivateRule(ruleID);
  }

  @Override
  public void disableRule(long ruleID, boolean dropPendingCommands) throws IOException {
    checkIfActive();
    ssm.getRuleManager().DisableRule(ruleID, dropPendingCommands);
  }

  @Override
  public CommandInfo getCommandInfo(long commandID) throws IOException {
    checkIfActive();
    return ssm.getCommandExecutor().getCommandInfo(commandID);
  }

  @Override
  public List<CommandInfo> listCommandInfo(long rid, CommandState commandState) throws IOException {
    checkIfActive();
    return ssm.getCommandExecutor().listCommandsInfo(rid, commandState);
  }

  @Override
  public void activateCommand(long commandID) throws IOException {
    checkIfActive();
    ssm.getCommandExecutor().activateCommand(commandID);
  }

  @Override
  public void disableCommand(long commandID) throws IOException {
    checkIfActive();
    ssm.getCommandExecutor().disableCommand(commandID);
  }

  @Override
  public void deleteCommand(long commandID) throws IOException {
    checkIfActive();
    ssm.getCommandExecutor().deleteCommand(commandID);
  }
}
