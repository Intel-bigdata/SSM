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
import org.smartdata.metaservice.BackupMetaService;
import org.smartdata.metaservice.CmdletMetaService;
import org.smartdata.metaservice.CopyMetaService;
import org.smartdata.metaservice.MetaServiceException;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileDiff;
import org.smartdata.server.engine.cmdlet.Cmdlet;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class CopyScheduler extends AbstractService {
  static final Logger LOG =
      LoggerFactory.getLogger(CopyScheduler.class);

  private ScheduledExecutorService executorService;
  private CmdletManager cmdletManager;

  private CopyMetaService copyMetaService;
  private CmdletMetaService cmdletMetaService;
  private BackupMetaService backupMetaService;

  private List<BackUpInfo> backUpInfos;

  public CopyScheduler(ServerContext context) {
    super(context);

    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.copyMetaService = (CopyMetaService) context.getMetaService();
    this.cmdletMetaService = (CmdletMetaService) context.getMetaService();
    this.backupMetaService = (BackupMetaService) context.getMetaService();
  }

  @Override
  public void init() throws IOException {

  }

  @Override
  public void start() throws IOException {
    // TODO Enable this module later
    // executorService.scheduleAtFixedRate(
    //     new CopyScheduler.ScheduleTask(), 1000, 1000,
    //     TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    // TODO Enable this module later
    // executorService.shutdown();
  }

  private class ScheduleTask implements Runnable {

    private void syncRule() {
      try {
        backUpInfos = backupMetaService.listAllBackUpInfo();
      } catch (MetaServiceException e) {
        LOG.debug("Sync backUpInfos error", e);
      }
    }

    private boolean diffMerge(List<FileDiff> fileDiffs) {
      // TODO merge diffs
      for (FileDiff fileDiff: fileDiffs) {

        fileDiff.getDiffType();

        fileDiff.getSrc();

      }
      return true;
    }


    private void handleDeleteChain() {
      // TODO delete all cmdlets and add a delete cmdlet
    }

    private void handleRenameChain() {
      // TODO Copy and merge rename cmdlets
    }

    private void handleCopyChain() {
      // TODO Merge and split large files

    }

    private void processCmdletByRule(BackUpInfo backUpInfo) {
      long rid = backUpInfo.getRid();
      int end = 0;
      List<CmdletInfo> dryRunCmdlets = null;
      try {
        // Get all dry run cmdlets
        dryRunCmdlets = cmdletMetaService.getCmdletsTableItem(null,
            String.format("= %d", rid), CmdletState.DRYRUN);
      } catch (MetaServiceException e) {
        LOG.debug("Get latest dry run cmdlets error, rid={}", rid, e);
      }
      if (dryRunCmdlets == null || dryRunCmdlets.size() == 0) {
        LOG.debug("rid={}, empty dry run cmdlets ", rid);
        return;
      }
      // Handle dry run cmdlets
      // Mark them as pending (runnable) after pre-processing
      do {
        try {
          // TODO optimize this pre-processing
          // TODO Check namespace for current states
          cmdletMetaService
              .updateCmdlet(dryRunCmdlets.get(end).getCid(), rid,
                  CmdletState.PENDING);
        } catch (MetaServiceException e) {
          LOG.debug("rid={}, empty dry run cmdlets ", rid);
        }
        // Split Copy tasks according to delete and rename
        String currentParameter = dryRunCmdlets.get(end).getParameters();
        if (currentParameter.contains("delete") ||
            currentParameter.contains("rename")) {
          break;
        }
        end++;
      } while (true);
    }

    @Override
    public void run() {
      syncRule();
      // TODO check dryRun DR (Copy, Rename) cmdlets
      for (BackUpInfo backUpInfo : backUpInfos) {
        // Go through all backup rules
        processCmdletByRule(backUpInfo);
      }
    }

    private class FileChain {
      private String head;
      private String tail;
      private List<Long> cmdletChain;
      private int state;
    }
  }
}
