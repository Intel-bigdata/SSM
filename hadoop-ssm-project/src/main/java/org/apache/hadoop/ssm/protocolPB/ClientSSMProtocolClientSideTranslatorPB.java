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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.CheckRuleRequestProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.GetRuleInfoRequestProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.GetRuleInfoResponseProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.GetServiceStateRequestProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.ListRulesInfoRequestProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.RuleInfoProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.SubmitRuleRequestProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClientSSMProtocolClientSideTranslatorPB implements ClientSSMProtocol {
  final private ClientSSMProtocolPB rpcProxy;

  public ClientSSMProtocolClientSideTranslatorPB(ClientSSMProtocolPB proxy) {
    this.rpcProxy = proxy;
  }
  
  @Override
  public SSMServiceState getServiceState() {
    GetServiceStateRequestProto req =
        GetServiceStateRequestProto.newBuilder().build();
    int state = rpcProxy.getServiceState(null, req).getState();
    return SSMServiceState.fromValue(state);
  }

  @Override
  public RuleInfo getRuleInfo(long id) throws IOException {
    try {
      GetRuleInfoRequestProto req =
          GetRuleInfoRequestProto.newBuilder().setRuleId(id).build();
      GetRuleInfoResponseProto r = rpcProxy.getRuleInfo(null, req);
      return PBHelper.convert(r.getResult());
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public long submitRule(String rule, RuleState initState) throws IOException {
    try {
      SubmitRuleRequestProto req = SubmitRuleRequestProto.newBuilder()
          .setRule(rule).setInitState(PBHelper.convert(initState)).build();
      return rpcProxy.submitRule(null, req).getRuleId();
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void checkRule(String rule) throws IOException {
    try {
      CheckRuleRequestProto req = CheckRuleRequestProto.newBuilder()
          .setRule(rule).build();
      rpcProxy.checkRule(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public List<RuleInfo> listRulesInfo() throws IOException {
    try {
      ListRulesInfoRequestProto req = ListRulesInfoRequestProto.newBuilder()
          .build();
      List<RuleInfoProto> infoProtos =
          rpcProxy.listRulesInfo(null, req).getRulesInfoList();
      if (infoProtos == null) {
        return null;
      }
      List<RuleInfo> ret = new ArrayList<>();
      for (RuleInfoProto infoProto : infoProtos) {
        ret.add(PBHelper.convert(infoProto));
      }
      return ret;
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }
}