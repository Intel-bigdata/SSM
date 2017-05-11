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
import org.apache.hadoop.ssm.protocol.ClientSSMProto.RuleStateProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.SubmitRuleRequestProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProto.SubmitRuleResponseProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;
import org.apache.hadoop.ssm.rule.RuleInfo;

import java.io.IOException;

public class ClientSSMProtocolServerSideTranslatorPB implements
    ClientSSMProtocolPB, ClientSSMProto.protoService.BlockingInterface {
  final private ClientSSMProtocol server;

  public ClientSSMProtocolServerSideTranslatorPB(ClientSSMProtocol server) {
    this.server = server;
  }

  @Override
  public ClientSSMProto.StatusResultProto getServiceStatus(
      RpcController controller, ClientSSMProto.StatusParaProto request) {
    ClientSSMProto.StatusResultProto.Builder builder =
        ClientSSMProto.StatusResultProto.newBuilder();
    SSMServiceStates SSMServiceStates = server.getServiceStatus();
    builder.setSSMServiceState(SSMServiceStates.getState().name());
    return builder.build();
  }

  @Override
  public ClientSSMProto.RuleInfoResultProto getRuleInfo(RpcController controller,
      ClientSSMProto.RuleInfoParaProto para) {
    ClientSSMProto.RuleInfoResultProto.Builder builder
        = ClientSSMProto.RuleInfoResultProto.newBuilder();
    RuleInfo ruleInfo = server.getRuleInfo(para.getPara());
    RuleStateProto ruleStateProto = PBHelper.convert(ruleInfo.getState());
    ClientSSMProto.RuleInfoResultTypeProto.Builder rtBuilder = ClientSSMProto
        .RuleInfoResultTypeProto
        .newBuilder()
        .setId(ruleInfo.getId())
        .setSubmitTime(ruleInfo.getSubmitTime())
        .setRuleText(ruleInfo.getRuleText())
        .setRulestateProto(ruleStateProto)
        .setCountConditionChecked(ruleInfo.getNumChecked())
        .setCountConditionFulfilled(ruleInfo.getNumCmdsGen());
    builder.setResult(rtBuilder.build());
    return builder.build();
  }

  @Override
  public SubmitRuleResponseProto submitRule(RpcController controller,
      SubmitRuleRequestProto req) throws ServiceException {
    try {
      long ruleId = server.submitRule(req.getRule(),
          PBHelper.convert(req.getInitState()));
      return SubmitRuleResponseProto.newBuilder().setRuleId(ruleId).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

}