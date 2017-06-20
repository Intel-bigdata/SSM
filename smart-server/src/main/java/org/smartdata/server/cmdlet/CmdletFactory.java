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
package org.smartdata.server.cmdlet;

import org.smartdata.SmartContext;
import org.smartdata.actions.ActionRegistry;
import org.smartdata.actions.SmartAction;
import org.smartdata.actions.hdfs.HdfsAction;
import org.smartdata.server.cmdlet.message.LaunchAction;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;

import java.util.ArrayList;
import java.util.List;

public class CmdletFactory {
  private final SmartContext smartContext;
  private ActionRegistry actionRegistry;

  public CmdletFactory(SmartContext smartContext) {
    this.smartContext = smartContext;
    this.actionRegistry = ActionRegistry.instance();
  }

  public Cmdlet createCmdlet(LaunchCmdlet launchCmdlet) {
    List<SmartAction> actions = new ArrayList<>();
    for (LaunchAction action : launchCmdlet.getLaunchActions()) {
      actions.add(this.createAction(action));
    }
    Cmdlet cmdlet = new Cmdlet(actions.toArray(new SmartAction[0]), null);
    cmdlet.setId(launchCmdlet.getCmdletId());
    return cmdlet;
  }

  public SmartAction createAction(LaunchAction launchAction) {
    SmartAction smartAction = actionRegistry.createAction(launchAction.getActionType());
    if (smartAction == null) {
      return null;
    }
    smartAction.setContext(smartContext);
    smartAction.setArguments(launchAction.getArgs());
    if (smartAction instanceof HdfsAction) {
//      ((HdfsAction) smartAction).setDfsClient(
//        new SmartDFSClient(ssm.getNamenodeURI(),
//          smartContext.getConf(), getRpcServerAddress()));
    }
    smartAction.getActionStatus().setId(launchAction.getActionId());
    return smartAction;
  }
}
