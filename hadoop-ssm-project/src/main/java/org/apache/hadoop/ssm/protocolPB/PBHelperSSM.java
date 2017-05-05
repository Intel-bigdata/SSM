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
import org.apache.hadoop.ssm.rule.RuleInfo;
import org.apache.hadoop.ssm.rule.RuleState;

public class PBHelperSSM {

  public static ClientSSMProto.RuleStateProto convert(RuleState ruleState) {
    ClientSSMProto.RuleStateProto ruleStateProto;
    switch (ruleState) {
      case ACTIVE:
        ruleStateProto = ClientSSMProto.RuleStateProto.ACTIVE;
        break;
      case DRYRUN:
        ruleStateProto = ClientSSMProto.RuleStateProto.DRYRUN;
        break;
      case DISABLED:
        ruleStateProto = ClientSSMProto.RuleStateProto.DISABLED;
        break;
      case FINISHED:
        ruleStateProto = ClientSSMProto.RuleStateProto.FINISHED;
        break;
      default:
        ruleStateProto = null;
    }
    return ruleStateProto;
  }

  public static RuleState convert(ClientSSMProto.RuleStateProto ruleStateProto) {
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
    return ruleState;
  }

  public static ClientSSMProto.RuleInfoResultProto convert(RuleInfo ruleInfo) {
    ClientSSMProto.RuleInfoResultProto.Builder builder = ClientSSMProto
        .RuleInfoResultProto
        .newBuilder()
        .setId(ruleInfo.getId())
        .setSubmitTime(ruleInfo.getSubmitTime())
        .setRuleText(ruleInfo.getRuleText())
        .setRulestateProto(convert(ruleInfo.getState()))
        .setCountConditionChecked(ruleInfo.getCountConditionChecked())
        .setCountConditionFulfilled(ruleInfo.getCountConditionFulfilled());
    return builder.build();
  }

  public static RuleInfo convert(ClientSSMProto.RuleInfoResultProto ruleInfoProto) {
    RuleInfo.Builder builder = RuleInfo.newBuilder()
        .setId(ruleInfoProto.getId())
        .setSubmitTime(ruleInfoProto.getSubmitTime())
        .setRuleText(ruleInfoProto.getRuleText())
        .setState(convert(ruleInfoProto.getRulestateProto()))
        .setCountConditionChecked(ruleInfoProto.getCountConditionChecked())
        .setCountConditionFulfilled(ruleInfoProto.getCountConditionFulfilled());
    return builder.build();
  }


}
