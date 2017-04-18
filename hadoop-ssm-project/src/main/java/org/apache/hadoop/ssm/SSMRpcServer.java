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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolPB;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolServerSideTranslatorPB;

import java.io.IOException;
import java.net.InetSocketAddress;

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

    BlockingService clientSSMPbService = ClientSSMProto.StatusService
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

  @Override
  public int add(int para1, int para2) {
    return para1 + para2;
  }

  @Override
  public SSMServiceStates getServiceStatus() {
    SSMServiceStates ssmServiceStates
        = new SSMServiceStates(SSMServiceState.SAFEMODE);
    return ssmServiceStates;
  }
}
