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
package org.apache.hadoop.smart.protocolPB;


import com.google.protobuf.ServiceException;
import org.apache.hadoop.smart.CommandState;
import org.apache.hadoop.smart.command.actions.ActionType;
import org.apache.hadoop.smart.protocol.AdminServerProto.CommandInfoProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.RuleInfoProto;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.rule.RuleState;
import org.apache.hadoop.smart.command.CommandInfo;

import java.io.IOException;

public class PBHelper {
  private PBHelper() {
  }

  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  public static int convert(RuleState state) {
    return state.getValue();
  }

  public static RuleState convert(int state) {
    return RuleState.fromValue(state);
  }

  public static RuleInfoProto convert(RuleInfo info) {
    return RuleInfoProto.newBuilder().setId(info.getId())
        .setSubmitTime(info.getSubmitTime())
        .setLastCheckTime(info.getLastCheckTime())
        .setRuleText(info.getRuleText())
        .setNumChecked(info.getNumChecked())
        .setNumCmdsGen(info.getNumCmdsGen())
        .setRulestateProto(convert(info.getState())).build();
  }

  public static RuleInfo convert(RuleInfoProto proto) {
    return RuleInfo.newBuilder().setId(proto.getId())
        .setSubmitTime(proto.getSubmitTime())
        .setLastCheckTime(proto.getLastCheckTime())
        .setRuleText(proto.getRuleText())
        .setNumChecked(proto.getNumChecked())
        .setNumCmdsGen(proto.getNumCmdsGen())
        .setState(convert(proto.getRulestateProto())).build();
  }

  public static CommandInfo convert(CommandInfoProto proto) {
    return CommandInfo.newBuilder()
        .setCid(proto.getCid())
        .setRid(proto.getRid())
        .setActionType(ActionType.fromValue(proto.getActionType()))
        .setState(CommandState.fromValue(proto.getState()))
        .setParameters(proto.getParameters())
        .setGenerateTime(proto.getGenerateTime())
        .setStateChangedTime(proto.getStateChangedTime())
        .build();
  }

  public static CommandInfoProto convert(CommandInfo info) {
    return CommandInfoProto.newBuilder()
        .setCid(info.getCid())
        .setRid(info.getRid())
        .setActionType(info.getActionType().getValue())
        .setState(info.getState().getValue())
        .setParameters(info.getParameters())
        .setGenerateTime(info.getGenerateTime())
        .setStateChangedTime(info.getStateChangedTime())
        .build();
  }
}
