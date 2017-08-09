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
package org.smartdata.server.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.metaservice.CmdletMetaService;
import org.smartdata.metaservice.CopyMetaService;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CentralizedCopyScheduler extends AbstractService {
  static final Logger LOG = LoggerFactory.getLogger(CentralizedCopyScheduler.class);

  private ScheduledExecutorService executorService;
  private CmdletManager cmdletManager;

  private CopyMetaService copyMetaService;
  private CmdletMetaService cmdletMetaService;

  public CentralizedCopyScheduler(ServerContext context) {
    super(context);

    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.copyMetaService = (CopyMetaService) context.getMetaService();
    this.cmdletMetaService = (CmdletMetaService) context.getMetaService();
  }

  @Override
  public void init() throws IOException {

  }

  @Override
  public void start() throws IOException {
    executorService.scheduleAtFixedRate(
        new CentralizedCopyScheduler.ScheduleTask(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    executorService.shutdown();
  }

  private class ScheduleTask implements Runnable {

    @Override
    public void run() {
      // TODO check dryRun copy cmdlets

      // TODO Schedule these cmdlets and remove duplicate cmdlets

      // TODO Large file split
    }
  }
}
