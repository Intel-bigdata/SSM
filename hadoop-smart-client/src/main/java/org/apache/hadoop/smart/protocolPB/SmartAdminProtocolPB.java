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
import org.apache.hadoop.smart.protocol.AdminServerProto.CheckRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.CheckRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetRuleInfoRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetRuleInfoResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetServiceStateRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetServiceStateResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ListRulesInfoRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ListRulesInfoResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.SubmitRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.SubmitRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DeleteRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ActivateRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DisableRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DeleteRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ActivateRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DisableRuleRequestProto;

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