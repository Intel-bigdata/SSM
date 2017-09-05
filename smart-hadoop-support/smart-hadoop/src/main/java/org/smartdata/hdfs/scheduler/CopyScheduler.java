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
package org.smartdata.hdfs.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.metastore.ActionSchedulerService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CopyScheduler extends ActionSchedulerService {
  static final Logger LOG =
      LoggerFactory.getLogger(CopyScheduler.class);
  // Fixed rate scheduler
  private ScheduledExecutorService executorService;
  // Global variables
  private MetaStore metaStore;
  private List<BackUpInfo> backUpInfos;


  public CopyScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    return null;
  }


  public List<String> getSupportedActions() {
    return null;
  }

  public boolean onSubmit(ActionInfo actionInfo) {
    return true;
  }

  public void postSchedule(ActionInfo actionInfo, ScheduleResult result) {

  }

  public void onPreDispatch(LaunchAction action) {

  }

  public void onActionFinished(ActionInfo actionInfo) {

  }

  @Override
  public void init() throws IOException {

  }

  @Override
  public void start() throws IOException {
    // TODO Enable this module later
    executorService.scheduleAtFixedRate(
        new CopyScheduler.ScheduleTask(), 1000, 1000,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    // TODO Enable this module later
    executorService.shutdown();
  }

  public void forceSync(String src, String dest) throws IOException, MetaStoreException {
    List<FileInfo> srcFiles = metaStore.getFilesByPrefix(src);
    for (FileInfo fileInfo: srcFiles) {
      if (fileInfo.isdir()) {
        continue;
      }
      String fullPath = fileInfo.getPath();
      // TODO Replace src with dest
      // New diff
    }
  }

  private class ScheduleTask implements Runnable {

    private Map<Long, FileDiff> fileDiffBatch;
    private Map<String, FileChain> fileChainMap;

    public ScheduleTask() {
      fileChainMap = new HashMap<>();
    }

    private void syncRule() {
      try {
        backUpInfos = metaStore.listAllBackUpInfo();
      } catch (MetaStoreException e) {
        LOG.debug("Sync backUpInfos error", e);
      }
    }

    private void syncFileDiff() {
      fileDiffBatch = new HashMap<>();
      List<FileDiff> pendingDiffs = null;
      try {
        pendingDiffs = metaStore.getPendingDiff();
        diffMerge(pendingDiffs);
      } catch (MetaStoreException e) {
        LOG.debug("Sync fileDiffs error", e);
      }
    }

    private void diffMerge(List<FileDiff> fileDiffs) throws MetaStoreException {
      // Merge all existing fileDiffs into fileChains
      for (FileDiff fileDiff: fileDiffs) {
        FileChain fileChain;
        String src = fileDiff.getSrc();
        fileDiffBatch.put(fileDiff.getDiffId(), fileDiff);
        // Get or create fileChain
        if (fileChainMap.containsKey(src)) {
          fileChain = fileChainMap.get(src);
        } else {
          fileChain = new FileChain();
          fileChainMap.put(src, fileChain);
        }
        if (fileDiff.getDiffType() == FileDiffType.RENAME) {
          String dest = fileDiff.getParameters().get("-dest");
          fileChain.tail = dest;
          // Update key in map
          fileChainMap.remove(src);
          if (fileChainMap.containsKey(dest)) {
            // Merge with existing chain
            // Delete then rename and append
            fileChainMap.get(dest).merge(fileChain);
            fileChain = fileChainMap.get(dest);
          } else {
            fileChainMap.put(dest, fileChain);
          }
        } else if (fileDiff.getDiffType() == FileDiffType.DELETE) {
          fileChain.tail = src;
          // Remove key in map
          fileChain.delete();
          // fileChainMap.remove(src);
        }
        // Add file diff to fileChain
        fileChain.fileDiffChain.add(fileDiff.getDiffId());
      }
    }

    /*private void handleFileChain(FileChain fileChain) throws MetaStoreException {
      List<FileDiff> resultSet = new ArrayList<>();
      for (Long fid: fileChain.getFileDiffChain()) {
        // Current append diff
        FileDiff fileDiff = new FileDiff();
        fileDiff.setParameters(new HashMap<String, String>());
        FileDiff currFileDiff = fileDiffBatch.get(fid);
        // if (currFileDiff.getDiffType() == FileDiffType.APPEND) {
        //   fileDiff.getParameters().put("-length", currFileDiff.getParameters().get("-length"));
        // }
        if (currFileDiff.getDiffType() == FileDiffType.DELETE) {
          FileDiff deleteFileDiff = new FileDiff();
          deleteFileDiff.setSrc(currFileDiff.getSrc());
          resultSet.add(deleteFileDiff);
        } else if (currFileDiff.getDiffType() == FileDiffType.RENAME) {
          FileDiff renameFileDiff = new FileDiff();
          renameFileDiff.setSrc(currFileDiff.getSrc());
          resultSet.add(renameFileDiff);
          // Set current append src as renamed src
          fileDiff.setSrc(currFileDiff.getSrc());
        }
        metaStore.markFileDiffApplied(fid, FileDiffState.MERGED);
      }
      // copyMetaService.markFileDiffApplied();
      // Insert file diffs into tables
      for (FileDiff fileDiff: resultSet) {
        metaStore.insertFileDiff(fileDiff);
      }
    }*/


    private void processCmdletByRule(BackUpInfo backUpInfo) {
      long rid = backUpInfo.getRid();
      int end = 0;
      List<CmdletInfo> dryRunCmdlets = null;
      try {
        // Get all dry run cmdlets
        dryRunCmdlets = metaStore.getCmdlets(null,
            String.format("= %d", rid), CmdletState.DRYRUN);
      } catch (MetaStoreException e) {
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
          metaStore
              .updateCmdlet(dryRunCmdlets.get(end).getCid(), rid,
                  CmdletState.PENDING);
        } catch (MetaStoreException e) {
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
      private List<Long> fileDiffChain;
      private int state;

      FileChain() {
        this.fileDiffChain = new ArrayList<>();
        this.tail = null;
        this.head = null;
      }

      public FileChain(String curr) {
        this();
        this.head = curr;
      }

      public List<Long> getFileDiffChain() {
        return fileDiffChain;
      }

      public void setFileDiffChain(List<Long> fileDiffChain) {
        this.fileDiffChain = fileDiffChain;
      }

      public void rename(String src, String dest) {
        tail = dest;
      }

      public void delete() throws MetaStoreException {
        for (long did: fileDiffChain) {
          metaStore.markFileDiffApplied(did, FileDiffState.APPLIED);
        }
        fileDiffChain.clear();
      }

      public void merge(FileChain previousChain) {

      }
    }
  }
}
