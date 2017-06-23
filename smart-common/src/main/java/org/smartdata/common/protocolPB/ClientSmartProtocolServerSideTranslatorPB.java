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
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.CmdletState;
import org.smartdata.common.models.ActionInfo;
import org.smartdata.common.protocol.AdminServerProto;
import org.smartdata.common.protocol.AdminServerProto.CheckRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.CheckRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetRuleInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetRuleInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetServiceStateRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetServiceStateResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListRulesInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListRulesInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.RuleInfoProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DisableRuleResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DisableRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetCmdletInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetCmdletInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListCmdletInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListCmdletInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateCmdletResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DisableCmdletResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DisableCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteCmdletResponseProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.CmdletInfoProto;
import org.smartdata.common.protocol.AdminServerProto.GetActionInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetActionInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionInfoOfLastActionsResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionInfoOfLastActionsRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActionInfoProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitCmdletResponseProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionsSupportedResponseProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionsSupportedRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActionDescriptorProto;

import org.smartdata.common.protocol.ClientServerProto;
import org.smartdata.common.protocol.ClientServerProto.ReportFileAccessEventRequestProto;
import org.smartdata.common.protocol.ClientServerProto.ReportFileAccessEventResponseProto;
import org.smartdata.common.SmartServiceState;
import org.smartdata.common.protocol.SmartServerProtocols;
import org.smartdata.common.models.RuleInfo;
import org.smartdata.common.models.CmdletInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClientSmartProtocolServerSideTranslatorPB implements
    SmartServerProtocolPBs,
    AdminServerProto.protoService.BlockingInterface,
    ClientServerProto.protoService.BlockingInterface {
  final private SmartServerProtocols server;

  public ClientSmartProtocolServerSideTranslatorPB(SmartServerProtocols server) {
    this.server = server;
  }

  @Override
  public GetServiceStateResponseProto getServiceState(RpcController controller,
      GetServiceStateRequestProto req) throws ServiceException {
    SmartServiceState s = null;
    try {
      s = server.getServiceState();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetServiceStateResponseProto.newBuilder()
        .setState(s.getValue()).build();
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

  @Override
  public CheckRuleResponseProto checkRule(RpcController controller,
      CheckRuleRequestProto req) throws ServiceException {
    try {
      server.checkRule(req.getRule());
      return CheckRuleResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetRuleInfoResponseProto getRuleInfo(RpcController controller,
      GetRuleInfoRequestProto req) throws ServiceException {
    try {
      RuleInfo info = server.getRuleInfo(req.getRuleId());
      return GetRuleInfoResponseProto.newBuilder()
          .setResult(PBHelper.convert(info)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListRulesInfoResponseProto listRulesInfo(RpcController controller,
      ListRulesInfoRequestProto req) throws ServiceException {
    try {
      List<RuleInfo> infos = server.listRulesInfo();
      List<RuleInfoProto> infoProtos = new ArrayList<>();
      for (RuleInfo info : infos) {
        infoProtos.add(PBHelper.convert(info));
      }
      return ListRulesInfoResponseProto.newBuilder()
          .addAllRulesInfo(infoProtos).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteRuleResponseProto deleteRule(RpcController controller,
      DeleteRuleRequestProto req) throws ServiceException {
    try {
      server.deleteRule(req.getRuleId(), req.getDropPendingCmdlets());
      return DeleteRuleResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ActivateRuleResponseProto activateRule(RpcController controller,
      ActivateRuleRequestProto req) throws ServiceException {
    try {
      server.activateRule(req.getRuleId());
      return ActivateRuleResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DisableRuleResponseProto disableRule(RpcController controller,
      DisableRuleRequestProto req) throws ServiceException {
    try {
      server.disableRule(req.getRuleId(), req.getDropPendingCmdlets());
      return DisableRuleResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetCmdletInfoResponseProto getCmdletInfo(
      RpcController controller, GetCmdletInfoRequestProto req)
      throws ServiceException {
    try {
      CmdletInfo cmdletInfo = server.getCmdletInfo(req.getCmdletID());
      return GetCmdletInfoResponseProto
          .newBuilder()
          .setCmdletInfo(PBHelper.convert(cmdletInfo))
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListCmdletInfoResponseProto listCmdletInfo(
      RpcController controller, ListCmdletInfoRequestProto req)
      throws ServiceException {
    try {
      List<CmdletInfo> list = server.listCmdletInfo(req.getRuleID(),
          CmdletState.fromValue(req.getCmdletState()));
      if (list == null)
        return ListCmdletInfoResponseProto.newBuilder().build();
      List<CmdletInfoProto> protoList = new ArrayList<>();
      for (CmdletInfo info : list) {
        protoList.add(PBHelper.convert(info));
      }
      return ListCmdletInfoResponseProto.newBuilder()
          .addAllCmdletInfos(protoList).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ActivateCmdletResponseProto activateCmdlet(
      RpcController controller, ActivateCmdletRequestProto req)
      throws ServiceException {
    try {
      server.activateCmdlet(req.getCmdletID());
      return ActivateCmdletResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DisableCmdletResponseProto disableCmdlet(
      RpcController controller,
      DisableCmdletRequestProto req)
      throws ServiceException {
    try {
      server.disableCmdlet(req.getCmdletID());
      return DisableCmdletResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteCmdletResponseProto deleteCmdlet(
      RpcController controller, DeleteCmdletRequestProto req)
      throws ServiceException {
    try {
      server.deleteCmdlet(req.getCmdletID());
      return DeleteCmdletResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetActionInfoResponseProto getActionInfo(RpcController controller,
      GetActionInfoRequestProto request) throws ServiceException {
    try {
      ActionInfo aI = server.getActionInfo(request.getActionID());
      return GetActionInfoResponseProto.newBuilder()
          .setActionInfo(PBHelper.convert(aI)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListActionInfoOfLastActionsResponseProto listActionInfoOfLastActions(
      RpcController controller, ListActionInfoOfLastActionsRequestProto request)
      throws ServiceException {
    try {
      List<ActionInfo> list =
          server.listActionInfoOfLastActions(request.getMaxNumActions());
      if (list==null) {
        return ListActionInfoOfLastActionsResponseProto.newBuilder().build();
      }
      List<ActionInfoProto> protoList = new ArrayList<>();
      for (ActionInfo a:list){
        protoList.add(PBHelper.convert(a));
      }
      return ListActionInfoOfLastActionsResponseProto.newBuilder()
          .addAllActionInfoList(protoList).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SubmitCmdletResponseProto submitCmdlet(RpcController controller,
      SubmitCmdletRequestProto req) throws ServiceException {
    try {
      long id = server.submitCmdlet(req.getCmd());
      return SubmitCmdletResponseProto.newBuilder()
          .setRes(id).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListActionsSupportedResponseProto listActionsSupported(
      RpcController controller, ListActionsSupportedRequestProto req)
      throws ServiceException {
    try {
      List<ActionDescriptor> adList = server.listActionsSupported();
      List<ActionDescriptorProto> prolist = new ArrayList<>();
      for (ActionDescriptor a : adList) {
        prolist.add(PBHelper.convert(a));
      }
      return ListActionsSupportedResponseProto.newBuilder()
          .addAllActDesList(prolist)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReportFileAccessEventResponseProto reportFileAccessEvent(
      RpcController controller, ReportFileAccessEventRequestProto req)
      throws ServiceException {
    try {
      server.reportFileAccessEvent(PBHelper.convert(req));
      return ReportFileAccessEventResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}