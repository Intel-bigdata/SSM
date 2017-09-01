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
import org.smartdata.server.engine.*;
import org.smartdata.server.engine.cmdlet.message.LaunchCmdlet;

import java.util.ArrayList;
import java.util.List;

//Todo: extract the onSchedule implementation
public class CmdletDispatcher {
  private Logger LOG = LoggerFactory.getLogger(CmdletDispatcher.class);
  private List<CmdletExecutorService> executorServices;
  private int index;

  public CmdletDispatcher(SmartContext smartContext, CmdletManager cmdletManager) {
    //Todo: make service configurable
    this.executorServices = new ArrayList<>();
    this.executorServices.add(new LocalCmdletExecutorService(smartContext.getConf(), cmdletManager));
    this.index = 0;
  }

  public void registerExecutorService(CmdletExecutorService executorService) {
    this.executorServices.add(executorService);
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
      CmdletExecutorService selected = executorServices.get(index % executorServices.size());
      selected.execute(cmdlet);
      LOG.info(
          String.format(
              "Dispatching cmdlet %s to executor service %s",
              cmdlet.getCmdletId(), selected.getClass()));
      index += 1;
    }
  }

  //Todo: pick the right service to stop cmdlet
  public void stop(long cmdletId) {
    for (CmdletExecutorService service : executorServices) {
      service.stop(cmdletId);
    }
  }

  //Todo: move this function to a proper place
  public void shutDownExcutorServices() {
    for (CmdletExecutorService service : executorServices) {
      service.shutdown();
    }
  }
}
