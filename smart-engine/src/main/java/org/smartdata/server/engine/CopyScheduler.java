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

import org.apache.commons.collections.map.HashedMap;
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
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileDiffType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

  public void forceSync(String src, String dest) throws IOException, MetaServiceException {
    // TODO check dest statuses to avoid unnecessary copy
    // Force Sync src and dest
    // TODO get namespace from file table
    // Classify files according to rules and back_up
    // Add filediffs to file diff table
  }

  private class ScheduleTask implements Runnable {

    private Map<Long, FileDiff> fileDiffBatch;

    private void syncRule() {
      try {
        backUpInfos = backupMetaService.listAllBackUpInfo();
      } catch (MetaServiceException e) {
        LOG.debug("Sync backUpInfos error", e);
      }
    }

    private void syncFileDiff() {
      List pendingDiffs = null;
      try {
        pendingDiffs = copyMetaService.getPendingDiff();
        diffMerge(pendingDiffs);
      } catch (MetaServiceException e) {
        LOG.debug("Sync fileDiffs error", e);
      }

    }

    private String getDestFromParameter(String parameter) {
      return "";
    }

    private void diffMerge(List<FileDiff> fileDiffs) throws MetaServiceException {
      // Merge all existing fileDiffs into fileChains
      Map<String, FileChain> fileChainMap = new HashMap<>();
      for (FileDiff fileDiff: fileDiffs) {
        FileChain fileChain;
        String src = fileDiff.getSrc();
        if (fileChainMap.containsKey(src)) {
          fileChain = fileChainMap.get(src);
        } else {
          fileChain = new FileChain();
          fileChainMap.put(src, fileChain);
        }
        if (fileDiff.getDiffType() == FileDiffType.RENAME) {
          String dest = getDestFromParameter(fileDiff.getParameters());
          fileChain.tail = dest;
          // Update key in map
          fileChainMap.remove(src);
          fileChainMap.put(dest, fileChain);
        }
        fileChain.fillDiffChain.add(fileDiff.getDiffId());
      }
      // Handle fileChains
      for (FileChain fileChain: fileChainMap.values()) {
        handleFileChain(fileChain);
      }
    }

    private void handleFileChain(FileChain fileChain) throws MetaServiceException {
      List<FileDiff> resultSet = new ArrayList<>();
      for (Long fid: fileChain.getFillDiffChain()) {
        // TODO get parameter map from parameters string
        Map<String, String> parameters = new HashMap<>();
        // Current append diff
        FileDiff fileDiff = new FileDiff();
        FileDiff currFileDiff = fileDiffBatch.get(fid);
        if (currFileDiff.getDiffType() == FileDiffType.APPEND) {
          // TODO Add incremental length to current
          fileDiff.setParameters(parameters.get("length"));
        } else if (currFileDiff.getDiffType() == FileDiffType.DELETE) {
          FileDiff deleteFileDiff = new FileDiff();
          // TODO add deleteFileDiff content
          deleteFileDiff.setSrc(currFileDiff.getSrc());
          resultSet.add(deleteFileDiff);
        } else if (currFileDiff.getDiffType() == FileDiffType.RENAME) {
          fileDiff.setSrc("");
        }
        copyMetaService.markFileDiffApplied(fid, FileDiffState.MERGED);
      }
      // copyMetaService.markFileDiffApplied();
      // Insert filediff into tables
      for (FileDiff fileDiff: resultSet) {
        copyMetaService.insertFileDiff(fileDiff);
      }
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
      // Sync backup rules
      syncRule();
      // Sync/schedule file diffs
      syncFileDiff();
      // Schedule backup cmdlets
      for (BackUpInfo backUpInfo : backUpInfos) {
        // Go through all backup rules
        processCmdletByRule(backUpInfo);
      }
    }

    private class FileChain {
      private String head;
      private String tail;
      private List<Long> fillDiffChain;
      private int state;

      FileChain() {
        this.fillDiffChain = new ArrayList<>();
        this.tail = null;
        this.head = null;
      }

      public FileChain(String curr) {
        this();
        this.head = curr;
      }

      public List<Long> getFillDiffChain() {
        return fillDiffChain;
      }

      public void setFillDiffChain(List<Long> fillDiffChain) {
        this.fillDiffChain = fillDiffChain;
      }
    }
  }
}
