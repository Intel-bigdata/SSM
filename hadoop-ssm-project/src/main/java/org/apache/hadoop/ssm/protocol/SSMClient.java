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
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolClientSideTranslatorPB;
import org.apache.hadoop.ssm.protocolPB.ClientSSMProtocolPB;

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

  public SSMClient(Configuration conf, InetSocketAddress address)
          throws IOException {
    this.conf = conf;
    RPC.setProtocolEngine(conf, ClientSSMProtocolPB.class, ProtobufRpcEngine.class);
    ClientSSMProtocolPB proxy = RPC.getProxy(
            ClientSSMProtocolPB.class, VERSION, address, conf);
    ClientSSMProtocol clientSSMProtocol =
            new ClientSSMProtocolClientSideTranslatorPB(proxy);
    this.ssm = clientSSMProtocol;
  }

  public int add(int para1, int para2) {
    return ssm.add(para1, para2);
  }

  public SSMServiceStates getServiceStatus() {
    return ssm.getServiceStatus();
  }
}
