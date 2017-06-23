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
package org.smartdata.admin.protocolPB;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.RPC;

import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.models.ActionInfo;
import org.smartdata.common.protocol.AdminServerProto.GetCmdletInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListCmdletInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DisableCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.CmdletInfoProto;
import org.smartdata.common.protocol.AdminServerProto.CheckRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetRuleInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.GetRuleInfoResponseProto;
import org.smartdata.common.protocol.AdminServerProto.GetServiceStateRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListRulesInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.RuleInfoProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateRuleRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DisableRuleRequestProto;
import org.smartdata.common.protocol.SmartAdminProtocol;
import org.smartdata.common.protocol.AdminServerProto.GetActionInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionInfoOfLastActionsRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActionInfoProto;
import org.smartdata.common.SmartServiceState;
import org.smartdata.common.protocolPB.PBHelper;
import org.smartdata.common.protocolPB.SmartAdminProtocolPB;
import org.smartdata.common.models.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.common.models.CmdletInfo;
import org.smartdata.common.CmdletState;
import org.smartdata.common.protocol.AdminServerProto.ActionDescriptorProto;
import org.smartdata.common.protocol.AdminServerProto.SubmitCmdletRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListActionsSupportedRequestProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SmartAdminProtocolAdminSideTranslatorPB implements
    java.io.Closeable, SmartAdminProtocol {
  private SmartAdminProtocolPB rpcProxy;

  public SmartAdminProtocolAdminSideTranslatorPB(SmartAdminProtocolPB proxy) {
    this.rpcProxy = proxy;
  }

  @Override
  public SmartServiceState getServiceState() throws IOException {
    GetServiceStateRequestProto req =
        GetServiceStateRequestProto.newBuilder().build();
    try {
      return SmartServiceState.fromValue(
          rpcProxy.getServiceState(null, req).getState());
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public RuleInfo getRuleInfo(long id) throws IOException {
    try {
      GetRuleInfoRequestProto req =
          GetRuleInfoRequestProto.newBuilder().setRuleId(id).build();
      GetRuleInfoResponseProto r = rpcProxy.getRuleInfo(null, req);
      return PBHelper.convert(r.getResult());
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public long submitRule(String rule, RuleState initState) throws IOException {
    try {
      SubmitRuleRequestProto req = SubmitRuleRequestProto.newBuilder()
          .setRule(rule).setInitState(PBHelper.convert(initState)).build();
      return rpcProxy.submitRule(null, req).getRuleId();
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void checkRule(String rule) throws IOException {
    try {
      CheckRuleRequestProto req = CheckRuleRequestProto.newBuilder()
          .setRule(rule).build();
      rpcProxy.checkRule(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public List<RuleInfo> listRulesInfo() throws IOException {
    try {
      ListRulesInfoRequestProto req = ListRulesInfoRequestProto.newBuilder()
          .build();
      List<RuleInfoProto> infoProtos =
          rpcProxy.listRulesInfo(null, req).getRulesInfoList();
      if (infoProtos == null) {
        return null;
      }
      List<RuleInfo> ret = new ArrayList<>();
      for (RuleInfoProto infoProto : infoProtos) {
        ret.add(PBHelper.convert(infoProto));
      }
      return ret;
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    DeleteRuleRequestProto req = DeleteRuleRequestProto.newBuilder()
        .setRuleId(ruleID)
        .setDropPendingCmdlets(dropPendingCmdlets)
        .build();
    try {
      rpcProxy.deleteRule(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void activateRule(long ruleID) throws IOException {
    ActivateRuleRequestProto req = ActivateRuleRequestProto.newBuilder()
        .setRuleId(ruleID).build();
    try {
      rpcProxy.activateRule(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void disableRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    DisableRuleRequestProto req = DisableRuleRequestProto.newBuilder()
        .setRuleId(ruleID)
        .setDropPendingCmdlets(dropPendingCmdlets)
        .build();
    try {
      rpcProxy.disableRule(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  // TODO Cmdlet RPC Client Interface
  @Override
  public CmdletInfo getCmdletInfo(long cmdletID) throws IOException {
    GetCmdletInfoRequestProto req = GetCmdletInfoRequestProto.newBuilder()
        .setCmdletID(cmdletID).build();
    try {
      return PBHelper.convert(rpcProxy.getCmdletInfo(null, req).getCmdletInfo());
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public List<CmdletInfo> listCmdletInfo(long rid, CmdletState cmdletState)
      throws IOException {
    ListCmdletInfoRequestProto req = ListCmdletInfoRequestProto.newBuilder()
        .setRuleID(rid).setCmdletState(cmdletState.getValue())
        .build();
    try {
      List<CmdletInfoProto> protoslist =
          rpcProxy.listCmdletInfo(null, req).getCmdletInfosList();
      if (protoslist == null)
        return new ArrayList<>();
      List<CmdletInfo> list = new ArrayList<>();
      for (CmdletInfoProto infoProto : protoslist) {
        list.add(PBHelper.convert(infoProto));
      }
      return list;
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void activateCmdlet(long cmdletID) throws IOException {
    try {
      ActivateCmdletRequestProto req = ActivateCmdletRequestProto.newBuilder()
          .setCmdletID(cmdletID)
          .build();
      rpcProxy.activateCmdlet(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }

  }

  @Override
  public void disableCmdlet(long cmdletID) throws IOException {
    try {
      DisableCmdletRequestProto req = DisableCmdletRequestProto.newBuilder()
          .setCmdletID(cmdletID)
          .build();
      rpcProxy.disableCmdlet(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteCmdlet(long cmdletID) throws IOException {
    try {
      DeleteCmdletRequestProto req = DeleteCmdletRequestProto.newBuilder()
          .setCmdletID(cmdletID)
          .build();
      rpcProxy.deleteCmdlet(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public ActionInfo getActionInfo(long actionID) throws IOException {
    GetActionInfoRequestProto req = GetActionInfoRequestProto.newBuilder()
        .setActionID(actionID)
        .build();
    try {
      return PBHelper.convert(rpcProxy.getActionInfo(null,req).getActionInfo());
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public List<ActionInfo> listActionInfoOfLastActions(int maxNumActions)
      throws IOException {
    ListActionInfoOfLastActionsRequestProto req =
        ListActionInfoOfLastActionsRequestProto.newBuilder()
        .setMaxNumActions(maxNumActions).build();
    try {
      List<ActionInfoProto> protoslist =
      rpcProxy.listActionInfoOfLastActions(null,req).getActionInfoListList();
      if (protoslist == null) {
        return new ArrayList<>();
      }
      List<ActionInfo> list = new ArrayList<>();
      for (ActionInfoProto infoProto : protoslist) {
        list.add(PBHelper.convert(infoProto));
      }
      return list;
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public long submitCmdlet(String cmd) throws IOException {
    SubmitCmdletRequestProto req = SubmitCmdletRequestProto.newBuilder()
        .setCmd(cmd).build();
    try {
     return rpcProxy.submitCmdlet(null,req).getRes();
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public List<ActionDescriptor> listActionsSupported() throws IOException {
    ListActionsSupportedRequestProto req = ListActionsSupportedRequestProto
        .newBuilder().build();
    try {
      List<ActionDescriptorProto> prolist = rpcProxy
          .listActionsSupported(null,req).getActDesListList();
      List<ActionDescriptor> list = new ArrayList<>();
      for(ActionDescriptorProto a:prolist){
        list.add(PBHelper.convert(a));
      }
      return list;
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
    rpcProxy = null;
  }
}