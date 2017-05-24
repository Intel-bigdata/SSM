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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.smart.CommandState;
import org.apache.hadoop.smart.protocol.ClientSmartProto.CheckRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetRuleInfoRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetRuleInfoResponseProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetServiceStateRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ListRulesInfoRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.RuleInfoProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.SubmitRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DeleteRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ActivateRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DisableRuleRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProtocol;
import org.apache.hadoop.smart.protocol.ClientSmartProto.GetCommandInfoRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ListCommandInfoRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.ActivateCommandRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DisableCommandRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.DeleteCommandRequestProto;
import org.apache.hadoop.smart.protocol.ClientSmartProto.CommandInfoProto;
import org.apache.hadoop.smart.protocol.SmartServiceState;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.rule.RuleState;
import org.apache.hadoop.smart.sql.CommandInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.smart.protocolPB.PBHelper.convert;

public class ClientSmartProtocolClientSideTranslatorPB implements
    java.io.Closeable, ClientSmartProtocol {
  private ClientSmartProtocolPB rpcProxy;

  public ClientSmartProtocolClientSideTranslatorPB(ClientSmartProtocolPB proxy) {
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
      return convert(r.getResult());
    } catch (ServiceException e) {
      throw PBHelper.getRemoteException(e);
    }
  }

  @Override
  public long submitRule(String rule, RuleState initState) throws IOException {
    try {
      SubmitRuleRequestProto req = SubmitRuleRequestProto.newBuilder()
          .setRule(rule).setInitState(convert(initState)).build();
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
        return new ArrayList<>();
      }
      List<RuleInfo> ret = new ArrayList<>();
      for (RuleInfoProto infoProto : infoProtos) {
        ret.add(convert(infoProto));
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
      return convert(rpcProxy.getCommandInfo(null, req).getCommandInfo());
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
        list.add(convert(infoProto));
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