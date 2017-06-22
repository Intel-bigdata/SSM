/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.smartdata.server;

import org.smartdata.AbstractService;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.engine.CmdletExecutor;
import org.smartdata.server.engine.ConfManager;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.ServerContext;
import org.smartdata.server.engine.StatesManager;

import java.io.IOException;

public class SmartEngine extends AbstractService {
  private ConfManager confMgr;
  private SmartConf conf;
  private ServerContext serverContext;
  private StatesManager statesMgr;
  private RuleManager ruleMgr;
  private CmdletExecutor cmdletExecutor;

  public SmartEngine(ServerContext context) {
    super(context);
    this.serverContext = context;
  }

  @Override
  public void init() throws IOException {
    statesMgr = new StatesManager(serverContext);
    cmdletExecutor = new CmdletExecutor(serverContext);
    ruleMgr = new RuleManager(serverContext, statesMgr, cmdletExecutor);
  }

  @Override
  public void start() throws IOException {

  }

  @Override
  public void stop() throws IOException {

  }

  public ConfManager getConfMgr() {
    return confMgr;
  }

  public SmartConf getConf() {
    return conf;
  }

  public StatesManager getStatesManager() {
    return statesMgr;
  }

  public RuleManager getRuleManager() {
    return ruleMgr;
  }

  public CmdletExecutor getCmdletExecutor() {
    return cmdletExecutor;
  }
}
