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


  public CopyScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.fileLock = new ConcurrentHashMap<>();
    this.actionDiffMap = new ConcurrentHashMap<>();
    this.fileChainMap = new HashMap<>();
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
        metaStore.markFileDiffApplied(did, FileDiffState.APPLIED);
        fileDiff = metaStore.getFileDiff(did);
        // Add to pending list
        if (fileChainMap.containsKey(fileDiff.getSrc())) {
          fileChainMap.get(fileDiff.getSrc()).addTopRunning();
        }
        if (actionDiffMap.containsKey(actionInfo.getActionId())) {
          actionDiffMap.remove(actionInfo.getActionId());
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
      for (FileDiff fileDiff : fileDiffs) {
        FileChain fileChain;
        String src = fileDiff.getSrc();
        // Skip applying file diffs
        if (actionDiffMap.containsValue(fileDiff.getDiffId())) {
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
        if (fileDiff.getDiffType() == FileDiffType.RENAME) {
          String dest = fileDiff.getParameters().get("-dest");
          fileChain.tail = dest;
          // Update key in map
          // fileChainMap.remove(src);
          // if (fileChainMap.containsKey(dest)) {
          //   // Merge with existing chain
          //   // Delete then rename and append
          //   fileChainMap.get(dest).merge(fileChain);
          //   fileChain = fileChainMap.get(dest);
          // } else {
          //   fileChainMap.put(dest, fileChain);
          // }
        } else if (fileDiff.getDiffType() == FileDiffType.DELETE) {
          fileChain.tail = src;
          // Remove key in map
          fileChain.clear();
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

    @Override
    public void run() {
      syncFileDiff();
      addToRunning();
    }

    private class FileChain {
      private String head;
      private String filePath;
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

      public void rename(String src, String dest) {
        tail = dest;
      }

      public void clear() throws MetaStoreException {
        for (long did : fileDiffChain) {
          metaStore.markFileDiffApplied(did, FileDiffState.APPLIED);
        }
        fileDiffChain.clear();
      }

      public void merge(FileChain previousChain) {

      }

      public long popTop() {
        if (fileDiffChain.size() == 0) {
          return -1;
        }
        long fid = fileDiffChain.get(0);
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
        metaStore.markFileDiffApplied(fid, FileDiffState.RUNNING);
        // fileLock.put(filePath, fid);
      }
    }
  }
}
