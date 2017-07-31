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
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.FileDiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class DisasterRecoveryManager extends AbstractService {
  static final Logger LOG = LoggerFactory.getLogger(DisasterRecoveryManager.class);

  private MetaStore metaStore;
  private Queue<FileDiff> pendingDR;
  private List<Long> runningDR;

  public DisasterRecoveryManager(ServerContext context) {
    super(context);

    this.metaStore = context.getMetaStore();
    this.runningDR = new ArrayList<>();
    this.pendingDR = new LinkedBlockingQueue<>();
  }

  public void fileDiff() {

  }

  @Override
  public void init() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

}
