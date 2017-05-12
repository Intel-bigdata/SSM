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
package org.apache.hadoop.ssm.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ssm.SSMConfigureKeys;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolClientSideTranslatorPB;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolPB;
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SSMClient {
  final static long VERSION = 1;
  Configuration conf;
  ClientSSMProtocol ssm;

  public boolean isClientRunning() {
    return clientRunning;
  }

  volatile boolean clientRunning = true;

  public SSMClient(Configuration conf)
      throws IOException {
    this.conf = conf;
    String[] strings = conf.get(SSMConfigureKeys.DFS_SSM_RPC_ADDRESS_KEY,
        SSMConfigureKeys.DFS_SSM_RPC_ADDRESS_DEFAULT).split(":");
    InetSocketAddress address = new InetSocketAddress(
        strings[strings.length - 2],
        Integer.parseInt(strings[strings.length - 1]));
    RPC.setProtocolEngine(conf, ClientSSMProtocolPB.class,
        ProtobufRpcEngine.class);
    ClientSSMProtocolPB proxy = RPC.getProxy(
        ClientSSMProtocolPB.class, VERSION, address, conf);
    ClientSSMProtocol clientSSMProtocol =
        new ClientSSMProtocolClientSideTranslatorPB(proxy);
    this.ssm = clientSSMProtocol;
  }

  public SSMServiceStates getServiceStatus() {
    return ssm.getServiceStatus();
  }

  public RuleInfo getRuleInfo(long id) {
    return ssm.getRuleInfo(id);
  }

  public long submitRule(String rule, RuleState initState)
      throws IOException {
    return ssm.submitRule(rule, initState);
  }
}
