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

import org.apache.hadoop.ssm.protocol.ClientSSMProto;
import org.apache.hadoop.ssm.protocol.ClientSSMProtocol;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.protocol.SSMServiceStates;
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

public class ClientSSMProtocolClientSideTranslatorPB implements ClientSSMProtocol {
  final private ClientSSMProtocolPB rpcProxy;

  public ClientSSMProtocolClientSideTranslatorPB(ClientSSMProtocolPB proxy) {
    this.rpcProxy = proxy;
  }
  
  @Override
  public SSMServiceStates getServiceStatus() {
    ClientSSMProto.StatusParaProto req =
        ClientSSMProto.StatusParaProto.newBuilder().build();
    String state = rpcProxy.getServiceStatus(null, req).getSSMServiceState();
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
    ClientSSMProto.RuleInfoResultTypeProto r =
        rpcProxy.getRuleInfo(null, req).getResult();
    RuleInfo.Builder builder = new RuleInfo.Builder();
    ClientSSMProto.RuleInfoResultTypeProto.RuleStateProto ruleStateProto =
        r.getRulestateProto();
    RuleState ruleState;
    switch (ruleStateProto) {
      case ACTIVE:
        ruleState = RuleState.ACTIVE;
        break;
      case DRYRUN:
        ruleState = RuleState.DRYRUN;
        break;
      case DISABLED:
        ruleState = RuleState.DISABLED;
        break;
      case FINISHED:
        ruleState = RuleState.FINISHED;
        break;
      default:
        ruleState = null;
    }
    builder.setId(r.getId())
        .setSubmitTime(r.getSubmitTime())
        .setRuleText(r.getRuleText())
        .setNumChecked(r.getCountConditionChecked())
        .setNumCmdsGen(r.getCountConditionFulfilled())
        .setState(ruleState);
    return builder.build();
  }
}