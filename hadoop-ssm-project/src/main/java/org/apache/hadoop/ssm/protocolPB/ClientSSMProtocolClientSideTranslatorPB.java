/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ssm.protocolPB;

import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;

public class ClientSSMProtocolClientSideTranslatorPB implements ClientSSMProtocol {
  final private ClientSSMProtocolPB rpcProxy;

  public ClientSSMProtocolClientSideTranslatorPB(ClientSSMProtocolPB proxy) {
    this.rpcProxy = proxy;

  }

  public int add(int para1, int para2) {
    // TODO Auto-generated method stub
    ClientSSMProto.AddParameters req = ClientSSMProto.AddParameters.newBuilder()
            .setPara1(para1).setPara2(para2).build();
    return rpcProxy.add(null, req).getResult();
  }

  @Override
  public SSMServiceStates getServiceStatus() {
    ClientSSMProto.StatusPara req = ClientSSMProto.StatusPara.newBuilder().build();
    String state = rpcProxy.getServiceStatus(null, req).getSSMServiceState();
    SSMServiceStates ssmServiceStates;
    if (state != null && state.equals(SSMServiceState.ACTIVE)) {
      ssmServiceStates = new SSMServiceStates(SSMServiceState.ACTIVE);
    } else {
      ssmServiceStates = new SSMServiceStates(SSMServiceState.SAFEMODE);
    }
    return ssmServiceStates;
  }

}