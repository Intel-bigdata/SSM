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

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolPB;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolServerSideTranslatorPB;
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the rpc calls.
 * TODO: Implement statistics for SSM rpc server
 */
public class SSMRpcServer implements ClientSSMProtocol {
  protected SSMServer ssm;
  protected Configuration conf;
  protected final InetSocketAddress clientRpcAddress;
  protected int serviceHandlerCount = 1;
  protected final RPC.Server clientRpcServer;

  public SSMRpcServer(SSMServer ssm, Configuration conf) throws IOException {
    this.ssm = ssm;
    this.conf = conf;
    // TODO: implement ssm ClientSSMProtocol
    InetSocketAddress rpcAddr = ssm.getRpcServerAddress(conf);
    RPC.setProtocolEngine(conf, ClientSSMProtocolPB.class, ProtobufRpcEngine.class);

    ClientSSMProtocolServerSideTranslatorPB clientSSMProtocolServerSideTranslatorPB
        = new ClientSSMProtocolServerSideTranslatorPB(this);

    BlockingService clientSSMPbService = ClientSSMProto.protoService
        .newReflectiveBlockingService(clientSSMProtocolServerSideTranslatorPB);

    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(ClientSSMProtocolPB.class)
        .setInstance(clientSSMPbService)
        .setBindAddress(rpcAddr.getHostName())
        .setPort(rpcAddr.getPort())
        .setNumHandlers(serviceHandlerCount)
        .setVerbose(true)
        .build();

    InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
    clientRpcAddress = new InetSocketAddress(
        rpcAddr.getHostName(), listenAddr.getPort());

    DFSUtil.addPBProtocol(conf, ClientSSMProtocolPB.class,
        clientSSMPbService, clientRpcServer);
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

  private void checkSafeMode() {
    if (getServiceStatus().getState() == SSMServiceState.SAFEMODE) {
      try {
        throw new SafeModeException("SSMServiceState is SAFEMODE !");
      } catch (SafeModeException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public SSMServiceStates getServiceStatus() {
    SSMServiceStates ssmServiceStates
        = new SSMServiceStates(SSMServiceState.SAFEMODE);
    return ssmServiceStates;
  }

  @Override
  public RuleInfo getRuleInfo(long id) {
    checkSafeMode();
    // nullpoint exeception
    /*RuleInfo ruleInfo = null;
    try {
      ruleInfo = ssm.getRuleManager().getRuleInfo(id);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ruleInfo;*/
    RuleInfo.Builder builder = RuleInfo.newBuilder();
    builder.setId(id)
        .setSubmitTime(6)
        .setRuleText("ruleTest")
        .setCountConditionChecked(7)
        .setCountConditionFulfilled(8)
        .setState(RuleState.ACTIVE);
    return builder.build();
  }

  @Override
  public List<RuleInfo> getAllRuleInfo() {
    checkSafeMode();
    List<RuleInfo> list = new ArrayList<>();
    RuleInfo.Builder builder = RuleInfo.newBuilder();
    builder.setId(5)
        .setSubmitTime(6)
        .setRuleText("ruleTest")
        .setCountConditionChecked(7)
        .setCountConditionFulfilled(8)
        .setState(RuleState.ACTIVE);
    list.add(builder.build());
    list.add(builder.build());
    return list;

/*    List<RuleInfo> list = new ArrayList<>();
    try {
      list = ssm.getRuleManager().getAllRuleInfo();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;*/
  }

  @Override
  public long submitRule(String rule, RuleState initState) {
    checkSafeMode();
    long res = 0L;
    try {
      res = ssm.getRuleManager().submitRule(rule, initState);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return res;
  }

  @Override
  public void checkRule(String rule) {
    checkSafeMode();
    try {
      ssm.getRuleManager().checkRule(rule);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void deleteRule(long ruleID, boolean dropPendingCommands) {
    checkSafeMode();
    System.out.println("delete rule");
  }

  @Override
  public void setRuleState(long ruleID, RuleState newState
      , boolean dropPendingCommands) {
    checkSafeMode();
    System.out.println("setRule State");
  }

}