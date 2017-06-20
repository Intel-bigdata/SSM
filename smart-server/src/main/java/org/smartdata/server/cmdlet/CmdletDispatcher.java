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
import org.smartdata.conf.SmartConf;
import org.smartdata.server.cmdlet.hazelcast.HazelcastExecutorService;
import org.smartdata.server.cmdlet.message.LaunchCmdlet;
import org.smartdata.server.cmdlet.executor.CmdletExecutorService;
import org.smartdata.server.cmdlet.executor.LocalCmdletExecutorService;

import java.util.ArrayList;
import java.util.List;

//Todo: extract the schedule implementation
public class CmdletDispatcher {
  private List<CmdletExecutorService> executorServices;
  private int index;

  public CmdletDispatcher(CmdletManager cmdletManager) {
    //Todo: make service configurable
    CmdletFactory factory = new CmdletFactory(new SmartContext(new SmartConf()));
    this.executorServices = new ArrayList<>();
    this.executorServices.add(new LocalCmdletExecutorService(cmdletManager, factory));
    this.executorServices.add(new HazelcastExecutorService(cmdletManager, factory));
    this.index = 0;
  }

  public boolean canDispatchMore() {
    for (CmdletExecutorService service : executorServices) {
      if (service.canAcceptMore()) {
        return true;
      }
    }
    return false;
  }

  public void dispatch(LaunchCmdlet cmdlet) {
    //Todo: optimize dispatching
    if (canDispatchMore()) {
      while (!executorServices.get(index % executorServices.size()).canAcceptMore()) {
        index += 1;
      }
      executorServices.get(index % executorServices.size()).execute(cmdlet);
      index += 1;
    }
  }
}
