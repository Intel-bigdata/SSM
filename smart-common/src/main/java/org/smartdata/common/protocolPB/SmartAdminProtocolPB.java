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
package org.smartdata.common.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.smartdata.common.protocol.AdminServerProto;
import org.smartdata.common.protocol.AdminServerProto.CheckRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.CheckRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetRuleInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetRuleInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetServiceStateRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetServiceStateResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListRulesInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListRulesInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DisableRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DisableRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetCommandInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetCommandInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListCommandInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListCommandInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateCommandResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateCommandRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DisableCommandResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DisableCommandRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteCommandResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteCommandRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetActionInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionInfoOfLastActionsResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetActionInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionInfoOfLastActionsRequestProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitCommandResponseProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitCommandRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionsSupportedResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionsSupportedRequestProto;

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
  
  GetCommandInfoResponseProto getCommandInfo(RpcController controller,
      GetCommandInfoRequestProto req) throws ServiceException;

  ListCommandInfoResponseProto listCommandInfo(RpcController controller,
      ListCommandInfoRequestProto req) throws ServiceException;

  ActivateCommandResponseProto activateCommand(RpcController controller,
      ActivateCommandRequestProto req) throws ServiceException;

  DisableCommandResponseProto disableCommand(RpcController controller,
      DisableCommandRequestProto req) throws ServiceException;

  DeleteCommandResponseProto deleteCommand(RpcController controller,
      DeleteCommandRequestProto req) throws ServiceException;

  GetActionInfoResponseProto getActionInfo(RpcController controller,
      GetActionInfoRequestProto req) throws ServiceException;

  ListActionInfoOfLastActionsResponseProto listActionInfoOfLastActions(
      RpcController controller, ListActionInfoOfLastActionsRequestProto req)
      throws ServiceException;

  SubmitCommandResponseProto submitCommand(
      RpcController controller, SubmitCommandRequestProto req)
      throws ServiceException;

  ListActionsSupportedResponseProto listActionsSupported(
      RpcController controller, ListActionsSupportedRequestProto req)
      throws ServiceException;

}