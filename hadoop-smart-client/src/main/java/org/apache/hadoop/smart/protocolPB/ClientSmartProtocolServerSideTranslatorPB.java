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
import org.apache.hadoop.smart.CommandState;
import org.apache.hadoop.smart.protocol.AdminServerProto.protoService.BlockingInterface;
import org.apache.hadoop.smart.protocol.AdminServerProto.CheckRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.CheckRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetRuleInfoRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetRuleInfoResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetServiceStateRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetServiceStateResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ListRulesInfoRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ListRulesInfoResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.RuleInfoProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.SubmitRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.SubmitRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DeleteRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ActivateRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DisableRuleResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DeleteRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ActivateRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DisableRuleRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetCommandInfoResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.GetCommandInfoRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ListCommandInfoResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ListCommandInfoRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ActivateCommandResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.ActivateCommandRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DisableCommandResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DisableCommandRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DeleteCommandResponseProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.DeleteCommandRequestProto;
import org.apache.hadoop.smart.protocol.AdminServerProto.CommandInfoProto;
import org.apache.hadoop.smart.protocol.SmartAdminProtocol;
import org.apache.hadoop.smart.protocol.SmartServiceState;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.sql.CommandInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.smart.protocolPB.PBHelper.convert;

public class ClientSmartProtocolServerSideTranslatorPB implements
    SmartAdminProtocolPB, SmartClientProtocolPB,
    BlockingInterface {
  final private SmartAdminProtocol server;

  public ClientSmartProtocolServerSideTranslatorPB(SmartAdminProtocol server) {
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
      server.deleteRule(req.getRuleId(), req.getDropPendingCommands());
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
      server.disableRule(req.getRuleId(), req.getDropPendingCommands());
      return DisableRuleResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetCommandInfoResponseProto getCommandInfo(
      RpcController controller, GetCommandInfoRequestProto req)
      throws ServiceException {
    try {
      CommandInfo commandInfo = server.getCommandInfo(req.getCommandID());
      return GetCommandInfoResponseProto
          .newBuilder()
          .setCommandInfo(convert(commandInfo))
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListCommandInfoResponseProto listCommandInfo(
      RpcController controller, ListCommandInfoRequestProto req)
      throws ServiceException {
    try {
      List<CommandInfo> list = server.listCommandInfo(req.getRuleID(),
          CommandState.fromValue(req.getCommandState()));
      if (list == null)
        return ListCommandInfoResponseProto.newBuilder().build();
      List<CommandInfoProto> protoList = new ArrayList<>();
      for (CommandInfo info : list) {
        protoList.add(convert(info));
      }
      return ListCommandInfoResponseProto.newBuilder()
          .addAllCommandInfos(protoList).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ActivateCommandResponseProto activateCommand(
      RpcController controller, ActivateCommandRequestProto req)
      throws ServiceException {
    try {
      server.activateCommand(req.getCommandID());
      return ActivateCommandResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DisableCommandResponseProto disableCommand(
      RpcController controller,
      DisableCommandRequestProto req)
      throws ServiceException {
    try {
      server.disableCommand(req.getCommandID());
      return DisableCommandResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteCommandResponseProto deleteCommand(
      RpcController controller, DeleteCommandRequestProto req)
      throws ServiceException {
    try {
      server.deleteCommand(req.getCommandID());
      return DeleteCommandResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}