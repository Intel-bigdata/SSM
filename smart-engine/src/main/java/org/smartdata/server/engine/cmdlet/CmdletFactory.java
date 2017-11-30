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
package org.smartdata.server.engine.cmdlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.action.ActionException;
import org.smartdata.action.ActionRegistry;
import org.smartdata.action.SmartAction;
//import org.smartdata.alluxio.AlluxioUtil;
//import org.smartdata.alluxio.action.AlluxioAction;
//import alluxio.client.file.FileSystem;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.model.LaunchAction;
import org.smartdata.protocol.message.StatusReporter;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class CmdletFactory {
  static final Logger LOG = LoggerFactory.getLogger(CmdletFactory.class);
  private final SmartContext smartContext;
  private final StatusReporter reporter;

  public CmdletFactory(SmartContext smartContext) {
    this(smartContext, null);
  }

  public CmdletFactory(SmartContext smartContext, StatusReporter reporter) {
    this.smartContext = smartContext;
    this.reporter = reporter;
  }

  public Cmdlet createCmdlet(LaunchCmdlet launchCmdlet) throws ActionException {
    List<SmartAction> actions = new ArrayList<>();
    for (LaunchAction action : launchCmdlet.getLaunchActions()) {
      actions.add(createAction(action));
    }
    Cmdlet cmdlet = new Cmdlet(actions.toArray(new SmartAction[0]), reporter);
    cmdlet.setId(launchCmdlet.getCmdletId());
    return cmdlet;
  }

  public SmartAction createAction(LaunchAction launchAction) throws ActionException {
    SmartAction smartAction = ActionRegistry.createAction(launchAction.getActionType());
    smartAction.setContext(smartContext);
    smartAction.init(launchAction.getArgs());
    smartAction.setActionId(launchAction.getActionId());
    smartAction.setStatusReporter(reporter);
    if (smartAction instanceof HdfsAction) {
      try {
        ((HdfsAction) smartAction)
            .setDfsClient(
                new SmartDFSClient(
                    HadoopUtil.getNameNodeUri(smartContext.getConf()),
                    smartContext.getConf(),
                    getRpcServerAddress()));
      } catch (IOException e) {
        LOG.error("smartAction aid={} setDfsClient error", launchAction.getActionId(), e);
        throw new ActionException(e);
      }
    }
    /*
    else if (smartAction instanceof AlluxioAction) {
      FileSystem fs;
      try {
        fs =  AlluxioUtil.getAlluxioFs(smartContext);
      } catch (Exception e) {
        LOG.error("smartAction aid={} alluxio filesystem error", launchAction.getActionId(), e);
        throw new ActionException(e);
      }
      ((AlluxioAction) smartAction).setFileSystem(fs);
    }
    */
    return smartAction;
  }

  private InetSocketAddress getRpcServerAddress() {
    String[] strings =
        smartContext
            .getConf()
            .get(
                SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY,
                SmartConfKeys.SMART_SERVER_RPC_ADDRESS_DEFAULT)
            .split(":");
    return new InetSocketAddress(
        strings[strings.length - 2], Integer.parseInt(strings[strings.length - 1]));
  }
}
