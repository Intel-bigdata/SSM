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
package org.apache.hadoop.smart;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.smart.protocol.ClientSSMProtocol;
import org.apache.hadoop.smart.protocol.ClientSmartProto;
import org.apache.hadoop.smart.protocol.SmartServiceState;
import org.apache.hadoop.smart.protocolPB.ClientSmartProtocolPB;
import org.apache.hadoop.smart.protocolPB.ClientSmartProtocolServerSideTranslatorPB;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.rule.RuleState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Implements the rpc calls.
 * TODO: Implement statistics for SSM rpc server
 */
public class SmartRpcServer implements ClientSSMProtocol {
  protected SmartServer ssm;
  protected Configuration conf;
  protected final InetSocketAddress clientRpcAddress;
  protected int serviceHandlerCount = 1;
  protected final RPC.Server clientRpcServer;

  public SmartRpcServer(SmartServer ssm, Configuration conf) throws IOException {
    this.ssm = ssm;
    this.conf = conf;
    // TODO: implement ssm ClientSSMProtocol
    InetSocketAddress rpcAddr = getRpcServerAddress();
    RPC.setProtocolEngine(conf, ClientSmartProtocolPB.class, ProtobufRpcEngine.class);

    ClientSmartProtocolServerSideTranslatorPB clientSSMProtocolServerSideTranslatorPB
        = new ClientSmartProtocolServerSideTranslatorPB(this);

    BlockingService clientSSMPbService = ClientSmartProto.protoService
        .newReflectiveBlockingService(clientSSMProtocolServerSideTranslatorPB);

    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(ClientSmartProtocolPB.class)
        .setInstance(clientSSMPbService)
        .setBindAddress(rpcAddr.getHostName())
        .setPort(rpcAddr.getPort())
        .setNumHandlers(serviceHandlerCount)
        .setVerbose(true)
        .build();

    InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
    clientRpcAddress = new InetSocketAddress(
        rpcAddr.getHostName(), listenAddr.getPort());

    DFSUtil.addPBProtocol(conf, ClientSmartProtocolPB.class,
        clientSSMPbService, clientRpcServer);
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
}
