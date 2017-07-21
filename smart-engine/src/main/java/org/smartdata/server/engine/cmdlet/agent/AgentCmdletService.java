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
package org.smartdata.server.engine.cmdlet.agent;

import org.smartdata.AgentService;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.engine.cmdlet.CmdletExecutor;
import org.smartdata.server.engine.cmdlet.CmdletFactory;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.engine.cmdlet.message.StopCmdlet;

import java.io.IOException;

public class AgentCmdletService extends AgentService {
  public static final String NAME = "AgentCmdletService";
  private CmdletExecutor executor;
  private CmdletFactory factory;

  public AgentCmdletService() {
  }

  @Override
  public void init() throws IOException {
    SmartAgentContext context = (SmartAgentContext) getContext();
    SmartConf conf = context.getConf();
    this.executor = new CmdletExecutor(conf, context.getStatusReporter());
    this.factory = new CmdletFactory(context, context.getStatusReporter());
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void stop() throws IOException {
    executor.shutdown();
  }


  @Override
  public void execute(Message message) throws Exception {
    if (message instanceof LaunchCmdlet) {
      executor.execute(factory.createCmdlet((LaunchCmdlet) message));
    } else if (message instanceof StopCmdlet) {
      executor.stop(((StopCmdlet) message).getCmdletId());
    } else {
      throw new IllegalArgumentException("unknown message " + message);
    }
  }

  @Override
  public String getServiceName() {
    return NAME;
  }

}
