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
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;
import org.apache.hadoop.ssm.rule.RuleInfo;

import java.util.List;

public class ClientSSMProtocolServerSideTranslatorPB implements
    ClientSSMProtocolPB, ClientSSMProto.protoService.BlockingInterface {
  final private ClientSSMProtocol server;

  public ClientSSMProtocolServerSideTranslatorPB(ClientSSMProtocol server) {
    this.server = server;
  }

  @Override
  public ClientSSMProto.StatusResultProto getServiceStatus(
      RpcController controller, ClientSSMProto.voidProto p) {
    ClientSSMProto.StatusResultProto.Builder builder =
        ClientSSMProto.StatusResultProto.newBuilder();
    SSMServiceStates SSMServiceStates = server.getServiceStatus();
    builder.setSSMServiceState(SSMServiceStates.getState().name());
    return builder.build();
  }

  @Override
  public ClientSSMProto.RuleInfoResultProto getRuleInfo(
      RpcController controller, ClientSSMProto.RuleInfoParaProto para) {
    RuleInfo ruleInfo = server.getRuleInfo(para.getPara());
    return PBHelperSSM.convert(ruleInfo);
  }

  @Override
  public ClientSSMProto.AllRuleInfoResultProto getAllRuleInfo(
      RpcController controller, ClientSSMProto.voidProto request)
      throws ServiceException {
    ClientSSMProto.AllRuleInfoResultProto.Builder allRuleInfoBuilder
        = ClientSSMProto.AllRuleInfoResultProto.newBuilder();

    List<RuleInfo> ruleInfoList = server.getAllRuleInfo();
    for (RuleInfo r : ruleInfoList) {
      allRuleInfoBuilder.addResult(PBHelperSSM.convert(r));
    }
    return allRuleInfoBuilder.build();
  }

  @Override
  public ClientSSMProto.submitRuleResProto submitRule(RpcController controller
      , ClientSSMProto.submitRuleParaProto p) throws ServiceException {
    ClientSSMProto.submitRuleResProto.Builder builder =
        ClientSSMProto.submitRuleResProto.newBuilder();
    ClientSSMProto.RuleStateProto ruleStateProto = p.getInitStateProto();
    long res = server.submitRule(p.getRule(), PBHelperSSM.convert(ruleStateProto));
    builder.setResult(res);
    return builder.build();
  }

  @Override
  public ClientSSMProto.voidProto checkRule(RpcController controller
      , ClientSSMProto.checkRuleParaProto p) throws ServiceException {
    ClientSSMProto.voidProto.Builder builder =
        ClientSSMProto.voidProto.newBuilder();
    server.checkRule(p.getRule());
    return builder.build();
  }

  @Override
  public ClientSSMProto.voidProto deleteRule(RpcController controller
      , ClientSSMProto.deleteRuleParaProto p) throws ServiceException {
    ClientSSMProto.voidProto.Builder builder =
        ClientSSMProto.voidProto.newBuilder();
    server.deleteRule(p.getRuleID(), p.getDropPendingCommands());
    return builder.build();
  }

  @Override
  public ClientSSMProto.voidProto setRuleState(RpcController controller
      , ClientSSMProto.setRuleStateParaProto p) throws ServiceException {
    ClientSSMProto.voidProto.Builder builder =
        ClientSSMProto.voidProto.newBuilder();
    ClientSSMProto.RuleStateProto ruleStateProto = p.getNewStateProto();
    server.setRuleState(p.getRuleID(), PBHelperSSM.convert(ruleStateProto)
        , p.getDropPendingCommands());
    return builder.build();
  }

}