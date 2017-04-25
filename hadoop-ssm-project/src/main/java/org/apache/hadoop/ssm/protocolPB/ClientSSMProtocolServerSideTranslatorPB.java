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
package org.apache.hadoop.ssm.protocolPB;

import com.google.protobuf.RpcController;
import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;

public class ClientSSMProtocolServerSideTranslatorPB implements
    ClientSSMProtocolPB, ClientSSMProto.StatusService.BlockingInterface {
  final private ClientSSMProtocol server;

  public ClientSSMProtocolServerSideTranslatorPB(ClientSSMProtocol server) {
    this.server = server;
  }

  @Override
  public ClientSSMProto.StatusResult getServiceStatus(
      RpcController controller, ClientSSMProto.StatusPara request) {
    ClientSSMProto.StatusResult.Builder builder =
        ClientSSMProto.StatusResult.newBuilder();
    SSMServiceStates SSMServiceStates = server.getServiceStatus();
    builder.setSSMServiceState(SSMServiceStates.getState().name());
    return builder.build();
  }

  public ClientSSMProto.AddResult add(
      RpcController controller, ClientSSMProto.AddParameters p) {
    // TODO Auto-generated method stub
    ClientSSMProto.AddResult.Builder builder =
        ClientSSMProto.AddResult.newBuilder();
    int result = server.add(p.getPara1(), p.getPara2());
    builder.setResult(result);
    return builder.build();
  }
}