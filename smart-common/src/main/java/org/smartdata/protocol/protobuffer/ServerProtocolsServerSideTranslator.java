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
import org.smartdata.SmartServiceState;
import org.smartdata.model.ActionDescriptor;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileState;
import org.smartdata.model.RuleInfo;
import org.smartdata.protocol.AdminServerProto;
import org.smartdata.protocol.AdminServerProto.ActionDescriptorProto;
import org.smartdata.protocol.AdminServerProto.ActionInfoProto;
import org.smartdata.protocol.AdminServerProto.ActivateCmdletRequestProto;
import org.smartdata.protocol.AdminServerProto.ActivateCmdletResponseProto;
import org.smartdata.protocol.AdminServerProto.ActivateRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.ActivateRuleResponseProto;
import org.smartdata.protocol.AdminServerProto.CheckRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.CheckRuleResponseProto;
import org.smartdata.protocol.AdminServerProto.CmdletInfoProto;
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
import org.smartdata.protocol.AdminServerProto.RuleInfoProto;
import org.smartdata.protocol.AdminServerProto.SubmitCmdletRequestProto;
import org.smartdata.protocol.AdminServerProto.SubmitCmdletResponseProto;
import org.smartdata.protocol.AdminServerProto.SubmitRuleRequestProto;
import org.smartdata.protocol.AdminServerProto.SubmitRuleResponseProto;
import org.smartdata.protocol.ClientServerProto;
import org.smartdata.protocol.ClientServerProto.GetFileStateRequestProto;
import org.smartdata.protocol.ClientServerProto.GetFileStateResponseProto;
import org.smartdata.protocol.ClientServerProto.ReportFileAccessEventRequestProto;
import org.smartdata.protocol.ClientServerProto.ReportFileAccessEventResponseProto;
import org.smartdata.protocol.SmartServerProtocols;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ServerProtocolsServerSideTranslator implements
    ServerProtocolsProtoBuffer,
    AdminServerProto.protoService.BlockingInterface,
    ClientServerProto.protoService.BlockingInterface {
  private final SmartServerProtocols server;

  public ServerProtocolsServerSideTranslator(SmartServerProtocols server) {
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
          ProtoBufferHelper.convert(req.getInitState()));
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
          .setResult(ProtoBufferHelper.convert(info)).build();
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
        infoProtos.add(ProtoBufferHelper.convert(info));
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
          .setCmdletInfo(ProtoBufferHelper.convert(cmdletInfo))
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
      if (list == null) {
        return ListCmdletInfoResponseProto.newBuilder().build();
      }
      List<CmdletInfoProto> protoList = new ArrayList<>();
      for (CmdletInfo info : list) {
        protoList.add(ProtoBufferHelper.convert(info));
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
          .setActionInfo(ProtoBufferHelper.convert(aI)).build();
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
      if (list == null) {
        return ListActionInfoOfLastActionsResponseProto.newBuilder().build();
      }
      List<ActionInfoProto> protoList = new ArrayList<>();
      for (ActionInfo a:list){
        protoList.add(ProtoBufferHelper.convert(a));
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
        prolist.add(ProtoBufferHelper.convert(a));
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
      server.reportFileAccessEvent(ProtoBufferHelper.convert(req));
      return ReportFileAccessEventResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetFileStateResponseProto getFileState(RpcController controller,
      GetFileStateRequestProto req) throws ServiceException {
    try {
      String path = req.getFilePath();
      FileState fileState = server.getFileState(path);
      return ProtoBufferHelper.convert(fileState);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
