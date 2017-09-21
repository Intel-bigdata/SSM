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

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.action.SyncAction;
import org.smartdata.metastore.ActionSchedulerService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CopyScheduler extends ActionSchedulerService {
  static final Logger LOG =
      LoggerFactory.getLogger(CopyScheduler.class);

  private static final List<String> actions = Arrays.asList("sync");
  // Fixed rate scheduler
  private ScheduledExecutorService executorService;
  // Global variables
  private MetaStore metaStore;
  // <File path, file diff id>
  private Map<String, Long> fileLock;
  // <actionId, file diff id>
  private Map<Long, Long> actionDiffMap;
  // <File path, FileChain object>
  private Map<String, ScheduleTask.FileChain> fileChainMap;
  // <did, file diff>
  private Map<Long, FileDiff> fileDiffMap;
  // Merge append length threshold
  private long mergeLenTh = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT * 3;
  // Merge count length threshold
  private long mergeCountTh = 10;


  public CopyScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.fileLock = new ConcurrentHashMap<>();
    this.actionDiffMap = new ConcurrentHashMap<>();
    this.fileChainMap = new HashMap<>();
    this.fileDiffMap = new ConcurrentHashMap<>();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (!actionInfo.getActionName().equals("sync")) {
      return ScheduleResult.FAIL;
    }
    String srcDir = action.getArgs().get(SyncAction.SRC);
    String path = action.getArgs().get("-file");
    String destDir = action.getArgs().get(SyncAction.DEST);
    if (!fileChainMap.containsKey(path)) {
      return ScheduleResult.FAIL;
    }
    long fid = fileChainMap.get(path).popTop();
    if (fid == -1) {
      // FileChain is already empty
      return ScheduleResult.FAIL;
    }
    fileLock.put(path, fid);
    FileDiff fileDiff = null;
    try {
      fileDiff = metaStore.getFileDiff(fid);
    } catch (MetaStoreException e) {
      e.printStackTrace();
    }
    switch (fileDiff.getDiffType()) {
      case APPEND:
        action.setActionType("copy");
        action.getArgs().put("-dest", path.replace(srcDir, destDir));
        break;
      case DELETE:
        action.setActionType("delete");
        action.getArgs().put("-file", path.replace(srcDir, destDir));
        break;
      case RENAME:
        action.setActionType("rename");
        action.getArgs().put("-file", path.replace(srcDir, destDir));
        // TODO scope check
        String remoteDest = fileDiff.getParameters().get("-dest");
        action.getArgs().put("-dest", remoteDest.replace(srcDir, destDir));
        fileDiff.getParameters().remove("-dest");
        break;
      case METADATA:
        action.setActionType("metadata");
        action.getArgs().put("-file", path.replace(srcDir, destDir));
        break;
      default:
        break;
    }
    // Put all parameters into args
    action.getArgs().putAll(fileDiff.getParameters());
    actionDiffMap.put(actionInfo.getActionId(), fid);
    return ScheduleResult.SUCCESS;
  }

  public List<String> getSupportedActions() {
    return actions;
  }

  public boolean onSubmit(ActionInfo actionInfo) {
    String path = actionInfo.getArgs().get("-file");
    System.out.println("Submit file" + path + fileLock.keySet());
    LOG.debug("Submit file {} with lock {}", path, fileLock.keySet());
    if (fileLock.containsKey(path)) {
      return false;
    }
    return true;
  }

  public void postSchedule(ActionInfo actionInfo, ScheduleResult result) {
    if (result.equals(ScheduleResult.SUCCESS)) {
      actionInfo.getActionId();
    }
  }

  public void onPreDispatch(LaunchAction action) {
  }

  public void onActionFinished(ActionInfo actionInfo) {
    // Remove lock
    FileDiff fileDiff = null;
    if(actionInfo.isFinished()) {
      try {
        long did = actionDiffMap.get(actionInfo.getActionId());
        metaStore.updateFileDiff(did, FileDiffState.APPLIED);
        fileDiff = metaStore.getFileDiff(did);
        // Add to pending list
        if (fileChainMap.containsKey(fileDiff.getSrc())) {
          fileChainMap.get(fileDiff.getSrc()).addTopRunning();
        }
        if (actionDiffMap.containsKey(actionInfo.getActionId())) {
          actionDiffMap.remove(actionInfo.getActionId());
        }
        if (fileDiffMap.containsKey(fileDiff.getDiffId())) {
          fileDiffMap.remove(fileDiff.getDiffId());
        }
      } catch (MetaStoreException e) {
        LOG.error("Mark sync action in metastore failed!", e);
      } catch (Exception e) {
        LOG.error("Sync action error", e);
      }
    }
    if (fileDiff != null && fileLock.containsKey(fileDiff.getSrc())) {
      fileLock.remove(fileDiff.getSrc());
    }
  }

  @Override
  public void init() throws IOException {
  }

  @Override
  public void start() throws IOException {
    // TODO Enable this module later
    executorService.scheduleAtFixedRate(
        new CopyScheduler.ScheduleTask(), 0, 500,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    // TODO Enable this module later
    executorService.shutdown();
  }



  private class ScheduleTask implements Runnable {

    private Map<Long, FileDiff> fileDiffBatch;

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

    private void addToRunning() {
      if (fileDiffMap.size() == 0) {
        return;
      }
      for (FileChain fileChain: fileChainMap.values()) {
        try {
          fileChain.addTopRunning();
        } catch (MetaStoreException e) {
          LOG.debug("Add fileDiffs to Pending list error", e);
          continue;
        }
      }
    }

    private void diffMerge(List<FileDiff> fileDiffs) throws MetaStoreException {
      // Merge all existing fileDiffs into fileChains
      LOG.debug("Size of Pending diffs", fileDiffs.size());
      for (FileDiff fileDiff : fileDiffs) {
        FileChain fileChain;
        String src = fileDiff.getSrc();
        // Skip applying file diffs
        if (fileDiffMap.containsValue(fileDiff.getDiffId())) {
          continue;
        }
        // fileDiffBatch.put(fileDiff.getDiffId(), fileDiff);
        // Get or create fileChain
        if (fileChainMap.containsKey(src)) {
          fileChain = fileChainMap.get(src);
        } else {
          fileChain = new FileChain();
          fileChain.setFilePath(src);
          fileChainMap.put(src, fileChain);
        }
        fileChain.addToChain(fileDiff);
        fileDiffMap.put(fileDiff.getDiffId(), fileDiff);
      }
    }

    @Override
    public void run() {
      try {
        syncFileDiff();
        addToRunning();
      } catch (Exception e) {
        LOG.error("CopyScheduler Run Error", e);
      }
    }

    private class FileChain {
      private long currAppendLength;
      private String filePath;
      private List<Long> fileDiffChain;
      private List<Long> appendChain;
      private List<String> nameChain;
      private int state;

      FileChain() {
        this.fileDiffChain = new ArrayList<>();
        this.appendChain = new ArrayList<>();
        this.nameChain = new ArrayList<>();
        this.currAppendLength = 0;
      }

      public FileChain(String filePath) {
        this();
        this.filePath = filePath;
        this.nameChain.add(filePath);
      }

      public String getFilePath() {
        return filePath;
      }

      public void setFilePath(String filePath) {
        this.filePath = filePath;
      }

      public List<Long> getFileDiffChain() {
        return fileDiffChain;
      }

      public void setFileDiffChain(List<Long> fileDiffChain) {
        this.fileDiffChain = fileDiffChain;
      }

      public void clear() throws MetaStoreException {
        for (long did : fileDiffChain) {
          metaStore.updateFileDiff(did, FileDiffState.APPLIED);
        }
        fileDiffChain.clear();
      }

      public void merge(FileChain previousChain) {
      }

      public void addToChain(FileDiff fileDiff) throws MetaStoreException {
        long did = fileDiff.getDiffId();
        if (fileDiff.getDiffType() == FileDiffType.APPEND) {
          if (currAppendLength >= mergeLenTh || appendChain.size() >= mergeCountTh) {
            mergeAppend();
          }
          // Add Append to Append Chain
          appendChain.add(did);
          // Increase Append length
          currAppendLength += Long.valueOf(fileDiff.getParameters().get("-length"));
        } else if (fileDiff.getDiffType() == FileDiffType.RENAME) {
          // Add New Name to Name Chain
          nameChain.add(fileDiff.getParameters().get("-dest"));
        } else if (fileDiff.getDiffType() == FileDiffType.DELETE) {
          clear();
          if (nameChain.size() > 1) {
            fileDiff.setSrc(nameChain.get(0));
            metaStore.updateFileDiff(did, nameChain.get(0));
          }
        }
        fileDiffChain.add(did);
      }

      private void mergeAppend() throws MetaStoreException {
        if (fileLock.containsKey(filePath)) {
          return;
        }
        // Lock file to avoid File Chain being processed
        fileLock.put(filePath, -1L);
        long offset = Integer.MAX_VALUE;
        long length = 0;
        long lastAppend = -1;
        for (long did : appendChain) {
          FileDiff fileDiff = metaStore.getFileDiff(did);
          if (fileDiff != null && fileDiff.getState().getValue() != 2) {
            long currOffset = Long.valueOf(fileDiff.getParameters().get("-offset"));
            // Re-compute append length
            length += Long.valueOf(fileDiff.getParameters().get("-length"));
            metaStore.updateFileDiff(did, FileDiffState.APPLIED);
            if (offset > currOffset) {
              offset = currOffset;
            }
            lastAppend = did;
          }
        }
        FileDiff fileDiff = metaStore.getFileDiff(lastAppend);
        fileDiff.getParameters().put("-offset", "" + offset);
        fileDiff.getParameters().put("-length", "" + length);
        // Update fileDiff in metastore
        metaStore.updateFileDiff(fileDiff.getDiffId(),
            FileDiffState.RUNNING, fileDiff.getParametersJsonString());
        // Unlock file
        fileLock.remove(filePath);
        currAppendLength = 0;
        appendChain.clear();
      }

      private void mergeMeta() {
        if (fileLock.containsKey(filePath)) {
          return;
        }
      }

      private void renameChain() {
        // Rename action will effect all append actions
      }


      public long popTop() {
        if (fileDiffChain.size() == 0) {
          return -1;
        }
        long fid = fileDiffChain.get(0);
        if (appendChain.size() > 0 && fid == appendChain.get(0)) {
          appendChain.remove(0);
        }
        fileDiffChain.remove(0);
        if (fileDiffChain.size() == 0) {
          fileChainMap.remove(filePath);
        }
        return fid;
      }

      public void addTopRunning() throws MetaStoreException {
        if (fileLock.containsKey(filePath)) {
          return;
        }
        if (fileDiffChain.size() == 0) {
          return;
        }
        long fid = fileDiffChain.get(0);
        metaStore.updateFileDiff(fid, FileDiffState.RUNNING);
        // fileLock.put(filePath, fid);
      }
    }
  }
}
