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

import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.protocol.AdminServerProto.GetCommandInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ListCommandInfoRequestProto;
import org.smartdata.common.protocol.AdminServerProto.ActivateCommandRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DisableCommandRequestProto;
import org.smartdata.common.protocol.AdminServerProto.DeleteCommandRequestProto;
import org.smartdata.common.protocol.AdminServerProto.CommandInfoProto;
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
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.common.CommandState;

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
  public void deleteRule(long ruleID, boolean dropPendingCommands)
      throws IOException {
    DeleteRuleRequestProto req = DeleteRuleRequestProto.newBuilder()
        .setRuleId(ruleID)
        .setDropPendingCommands(dropPendingCommands)
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
  public void disableRule(long ruleID, boolean dropPendingCommands)
      throws IOException {
    DisableRuleRequestProto req = DisableRuleRequestProto.newBuilder()
        .setRuleId(ruleID)
        .setDropPendingCommands(dropPendingCommands)
        .build();
    try {
      rpcProxy.disableRule(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  // TODO Command RPC Client Interface
  @Override
  public CommandInfo getCommandInfo(long commandID) throws IOException {
    GetCommandInfoRequestProto req = GetCommandInfoRequestProto.newBuilder()
        .setCommandID(commandID).build();
    try {
      return PBHelper.convert(rpcProxy.getCommandInfo(null, req).getCommandInfo());
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public List<CommandInfo> listCommandInfo(long rid, CommandState commandState)
      throws IOException {
    ListCommandInfoRequestProto req = ListCommandInfoRequestProto.newBuilder()
        .setRuleID(rid).setCommandState(commandState.getValue())
        .build();
    try {
      List<CommandInfoProto> protoslist =
          rpcProxy.listCommandInfo(null, req).getCommandInfosList();
      if (protoslist == null)
        return new ArrayList<>();
      List<CommandInfo> list = new ArrayList<>();
      for (CommandInfoProto infoProto : protoslist) {
        list.add(PBHelper.convert(infoProto));
      }
      return list;
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void activateCommand(long commandID) throws IOException {
    try {
      ActivateCommandRequestProto req = ActivateCommandRequestProto.newBuilder()
          .setCommandID(commandID)
          .build();
      rpcProxy.activateCommand(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }

  }

  @Override
  public void disableCommand(long commandID) throws IOException {
    try {
      DisableCommandRequestProto req = DisableCommandRequestProto.newBuilder()
          .setCommandID(commandID)
          .build();
      rpcProxy.disableCommand(null, req);
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public void deleteCommand(long commandID) throws IOException {
    try {
      DeleteCommandRequestProto req = DeleteCommandRequestProto.newBuilder()
          .setCommandID(commandID)
          .build();
      rpcProxy.deleteCommand(null, req);
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