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
package org.smartdata.protocol.protobuffer;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;
import org.smartdata.SmartConstants;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.protocol.AdminServerProto.ActivateCmdletRequestProto;
import org.smartdata.protocol.AdminServerProto.ActivateCmdletResponseProto;
import org.smartdata.protocol.AdminServerProto.ActivateRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.ActivateRuleResponseProto;
import org.smartdata.protocol.AdminServerProto.CheckRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.CheckRuleResponseProto;
import org.smartdata.protocol.AdminServerProto.DeleteCmdletRequestProto;
import org.smartdata.protocol.AdminServerProto.DeleteCmdletResponseProto;
import org.smartdata.protocol.AdminServerProto.DeleteRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.DeleteRuleResponseProto;
import org.smartdata.protocol.AdminServerProto.DisableCmdletRequestProto;
import org.smartdata.protocol.AdminServerProto.DisableCmdletResponseProto;
import org.smartdata.protocol.AdminServerProto.DisableRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.DisableRuleResponseProto;
import org.smartdata.protocol.AdminServerProto.GetActionInfoRequestProto;
import org.smartdata.protocol.AdminServerProto.GetActionInfoResponseProto;
import org.smartdata.protocol.AdminServerProto.GetCmdletInfoRequestProto;
import org.smartdata.protocol.AdminServerProto.GetCmdletInfoResponseProto;
import org.smartdata.protocol.AdminServerProto.GetRuleInfoRequestProto;
import org.smartdata.protocol.AdminServerProto.GetRuleInfoResponseProto;
import org.smartdata.protocol.AdminServerProto.GetServiceStateRequestProto;
import org.smartdata.protocol.AdminServerProto.GetServiceStateResponseProto;
import org.smartdata.protocol.AdminServerProto.ListActionInfoOfLastActionsRequestProto;
import org.smartdata.protocol.AdminServerProto.ListActionInfoOfLastActionsResponseProto;
import org.smartdata.protocol.AdminServerProto.ListActionsSupportedRequestProto;
import org.smartdata.protocol.AdminServerProto.ListActionsSupportedResponseProto;
import org.smartdata.protocol.AdminServerProto.ListCmdletInfoRequestProto;
import org.smartdata.protocol.AdminServerProto.ListCmdletInfoResponseProto;
import org.smartdata.protocol.AdminServerProto.ListRulesInfoRequestProto;
import org.smartdata.protocol.AdminServerProto.ListRulesInfoResponseProto;
import org.smartdata.protocol.AdminServerProto.SubmitCmdletRequestProto;
import org.smartdata.protocol.AdminServerProto.SubmitCmdletResponseProto;
import org.smartdata.protocol.AdminServerProto.SubmitRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.SubmitRuleResponseProto;

@KerberosInfo(
  serverPrincipal = SmartConfKeys.SMART_SERVER_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(protocolName = SmartConstants.SMART_ADMIN_PROTOCOL_NAME,
    protocolVersion = 1)
public interface AdminProtocolProtoBuffer {

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

  GetCmdletInfoResponseProto getCmdletInfo(RpcController controller,
      GetCmdletInfoRequestProto req) throws ServiceException;

  ListCmdletInfoResponseProto listCmdletInfo(RpcController controller,
      ListCmdletInfoRequestProto req) throws ServiceException;

  ActivateCmdletResponseProto activateCmdlet(RpcController controller,
      ActivateCmdletRequestProto req) throws ServiceException;

  DisableCmdletResponseProto disableCmdlet(RpcController controller,
      DisableCmdletRequestProto req) throws ServiceException;

  DeleteCmdletResponseProto deleteCmdlet(RpcController controller,
      DeleteCmdletRequestProto req) throws ServiceException;

  GetActionInfoResponseProto getActionInfo(RpcController controller,
      GetActionInfoRequestProto req) throws ServiceException;

  ListActionInfoOfLastActionsResponseProto listActionInfoOfLastActions(
      RpcController controller, ListActionInfoOfLastActionsRequestProto req)
      throws ServiceException;

  SubmitCmdletResponseProto submitCmdlet(
      RpcController controller, SubmitCmdletRequestProto req)
      throws ServiceException;

  ListActionsSupportedResponseProto listActionsSupported(
      RpcController controller, ListActionsSupportedRequestProto req)
      throws ServiceException;
}
