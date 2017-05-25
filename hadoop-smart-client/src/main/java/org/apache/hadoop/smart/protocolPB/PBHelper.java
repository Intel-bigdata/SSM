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
import org.apache.hadoop.smart.protocol.ClientSmartProto.RuleInfoProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.RuleStateProto;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.rule.RuleState;

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

  public static RuleStateProto convert(RuleState state) {
    for (RuleStateProto v : RuleStateProto.values()) {
      if (v.getNumber() == state.getValue()) {
        return v;
      }
    }
    return null;
  }

  public static RuleState convert(RuleStateProto state) {
    for (RuleState v : RuleState.values()) {
      if (v.getValue() == state.getNumber()) {
        return v;
      }
    }
    return null;
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
}
