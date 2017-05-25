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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.smart.protocol.ClientSmartProto.CheckRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.CheckRuleResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetRuleInfoRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetRuleInfoResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetServiceStateRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetServiceStateResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ListRulesInfoRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ListRulesInfoResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.SubmitRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.SubmitRuleResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DeleteRuleResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ActivateRuleResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DisableRuleResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DeleteRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ActivateRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DisableRuleRequestProto;

@ProtocolInfo(protocolName = "org.apache.hadoop.ssm.protocolPB.SmartAdminProtocolPB",
    protocolVersion = 1)
public interface SmartAdminProtocolPB {

  GetServiceStateResponseProto
  getServiceState(RpcController controller,
      GetServiceStateRequestProto req) throws ServiceException;

  GetRuleInfoResponseProto getRuleInfo(RpcController controller,
      GetRuleInfoRequestProto req) throws ServiceException;

  SubmitRuleResponseProto submitRule(RpcController controller,
      SubmitRuleRequestProto req) throws ServiceException;

  CheckRuleResponseProto checkRule(RpcController controller,
      CheckRuleRequestProto req) throws ServiceException;

  ListRulesInfoResponseProto listRulesInfo(RpcController controller,
      ListRulesInfoRequestProto req) throws ServiceException;

  DeleteRuleResponseProto deleteRule(RpcController controller,
      DeleteRuleRequestProto req) throws ServiceException;

  ActivateRuleResponseProto activateRule(RpcController controller,
      ActivateRuleRequestProto req) throws ServiceException;

  DisableRuleResponseProto disableRule(RpcController controller,
      DisableRuleRequestProto req) throws ServiceException;
}