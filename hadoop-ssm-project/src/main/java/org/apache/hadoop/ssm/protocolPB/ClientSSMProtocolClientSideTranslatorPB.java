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
import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

import java.util.ArrayList;
import java.util.List;

public class ClientSSMProtocolClientSideTranslatorPB implements ClientSSMProtocol {
  final private ClientSSMProtocolPB rpcProxy;

  public ClientSSMProtocolClientSideTranslatorPB(ClientSSMProtocolPB proxy) {
    this.rpcProxy = proxy;
  }

  @Override
  public SSMServiceStates getServiceStatus() {
    ClientSSMProto.voidProto req =
        ClientSSMProto.voidProto.newBuilder().build();
    String state = null;
    try {
      state = rpcProxy.getServiceStatus(null, req).getSSMServiceState();
    } catch (ServiceException e) {
      e.printStackTrace();
    }
    SSMServiceStates ssmServiceStates;
    if (state != null && state.equals(SSMServiceState.ACTIVE)) {
      ssmServiceStates = new SSMServiceStates(SSMServiceState.ACTIVE);
    } else {
      ssmServiceStates = new SSMServiceStates(SSMServiceState.SAFEMODE);
    }
    return ssmServiceStates;
  }

  @Override
  public RuleInfo getRuleInfo(long id) {
    ClientSSMProto.RuleInfoParaProto req =
        ClientSSMProto.RuleInfoParaProto.newBuilder().setPara(id).build();
    ClientSSMProto.RuleInfoResultProto r =
        null;
    try {
      r = rpcProxy.getRuleInfo(null, req);
    } catch (ServiceException e) {
      e.printStackTrace();
    }
    return PBHelperSSM.convert(r);
  }

  @Override
  public List<RuleInfo> getAllRuleInfo() {
    ClientSSMProto.voidProto req = ClientSSMProto.voidProto.newBuilder().build();
    ClientSSMProto.AllRuleInfoResultProto allRuleInfoProto = null;
    try {
      allRuleInfoProto = rpcProxy.getAllRuleInfo(null, req);
    } catch (ServiceException e) {
      e.printStackTrace();
    }
    List<RuleInfo> ruleInfoList = new ArrayList<>();
    for (int i = 0; i < allRuleInfoProto.getResultCount(); i++) {
      ruleInfoList.add(PBHelperSSM.convert(allRuleInfoProto.getResult(i)));
    }
    return ruleInfoList;
  }

  @Override
  public long submitRule(String rule, RuleState initState) {
    ClientSSMProto.submitRuleParaProto req =
        ClientSSMProto.submitRuleParaProto.newBuilder().setRule(rule)
            .setInitStateProto(PBHelperSSM.convert(initState)).build();
    long res = 0;
    try {
      res = rpcProxy.submitRule(null, req).getResult();
    } catch (ServiceException e) {
      e.printStackTrace();
    }
    return res;
  }

  @Override
  public void checkRule(String rule) {
    ClientSSMProto.checkRuleParaProto req =
        ClientSSMProto.checkRuleParaProto.newBuilder().setRule(rule).build();
    try {
      rpcProxy.checkRule(null, req);
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void deleteRule(long ruleID, boolean dropPendingCommands) {
    ClientSSMProto.deleteRuleParaProto req =
        ClientSSMProto.deleteRuleParaProto.newBuilder().setRuleID(ruleID)
            .setDropPendingCommands(dropPendingCommands).build();
    try {
      rpcProxy.deleteRule(null, req);
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setRuleState(long ruleID, RuleState newState, boolean dropPendingCommands) {
    ClientSSMProto.setRuleStateParaProto req =
        ClientSSMProto.setRuleStateParaProto.newBuilder()
            .setRuleID(ruleID)
            .setNewStateProto(PBHelperSSM.convert(newState))
            .setDropPendingCommands(dropPendingCommands)
            .build();
    try {
      rpcProxy.setRuleState(null, req);
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

}