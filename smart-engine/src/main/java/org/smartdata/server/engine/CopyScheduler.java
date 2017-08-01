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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileDiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CopyScheduler extends AbstractService {
  static final Logger LOG = LoggerFactory.getLogger(CopyScheduler.class);

  private ScheduledExecutorService executorService;

  private CmdletManager cmdletManager;
  private MetaStore metaStore;
  private Queue<FileDiff> pendingDR;
  private List<Long> runningDR;
  // TODO currently set max running list.size == 1 for test
  private final int MAX_RUNNING_SIZE = 1;

  public CopyScheduler(ServerContext context) {
    super(context);

    this.executorService = Executors.newSingleThreadScheduledExecutor();

    this.metaStore = context.getMetaStore();
    this.runningDR = new ArrayList<>();
    this.pendingDR = new LinkedBlockingQueue<>();
  }

  public CopyScheduler(ServerContext context, CmdletManager cmdletManager) {
    this(context);
    this.cmdletManager = cmdletManager;
  }

  public void diffMerge(List<FileDiff> fileDiffList) {

  }

  @Override
  public void init() throws IOException {
  }

  @Override
  public void start() throws IOException {
    executorService.scheduleAtFixedRate(
        new ScheduleTask(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    executorService.shutdown();
  }


  private class ScheduleTask implements Runnable {


    private void runningStatusUpdate() throws MetaStoreException {
      // Status update
      for (long cid : runningDR) {
        if (metaStore.getCmdletById(cid).getState() == CmdletState.DONE) {
          runningDR.remove(cid);
        }
      }
    }

    private void enQueue() throws IOException {
      // Move diffs to running queue
      while (runningDR.size() < MAX_RUNNING_SIZE) {
        FileDiff fileDiff = pendingDR.poll();
        // TODO parse and Submit cmdlet
        long cid = cmdletManager.submitCmdlet("Test");
        runningDR.add(cid);
      }
    }

    @Override
    public void run() {
      try {
        // Add new diffs to pending list
        List<FileDiff> latestFileDiff = metaStore.getLatestFileDiff();
        for (FileDiff fileDiff : latestFileDiff) {
          if (!pendingDR.contains(fileDiff)) {
            pendingDR.add(fileDiff);
          }
        }
        runningStatusUpdate();
        enQueue();

      } catch (IOException e) {
        LOG.error("Disaster Recovery Manager schedule error", e);
      } catch (MetaStoreException e) {
        LOG.error("Disaster Recovery Manager MetaStore error", e);
      }
    }
  }

}
