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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.action.SyncAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
  private Configuration conf;
  // <File path, file diff id>
  private Map<String, Long> fileLock;
  // <actionId, file diff id>
  private Map<Long, Long> actionDiffMap;
  // <File path, FileChain object>
  private Map<String, ScheduleTask.FileChain> fileDiffChainMap;
  // <did, Fail times>
  private Map<Long, Integer> fileDiffMap;
  // BaseSync queue
  private Map<String, String> baseSyncQueue;
  private Map<String, Boolean> overwriteQueue;
  // Merge append length threshold
  private long mergeLenTh = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT * 3;
  // Merge count length threshold
  private long mergeCountTh = 10;
  private int retryTh = 3;
  // Check interval of executorService
  private long checkInterval = 150;
  // Base sync batch insert size
  private int batchSize = 300;
  // Cache of the file_diff
  private Map<Long, FileDiff> fileDiffCache;
  // cache sync threshold, default 50
  private int cacheSyncTh = 50;
  // record the file_diff whether being changed
  private Map<Long, Boolean> fileDiffChanged;


  public CopyScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.fileLock = new ConcurrentHashMap<>();
    this.actionDiffMap = new ConcurrentHashMap<>();
    this.fileDiffChainMap = new HashMap<>();
    this.fileDiffMap = new ConcurrentHashMap<>();
    this.baseSyncQueue = new ConcurrentHashMap<>();
    this.overwriteQueue = new ConcurrentHashMap<>();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    try {
      conf = getContext().getConf();
    } catch (NullPointerException e) {
      // If SmartContext is empty
      conf = new Configuration();
    }
    this.fileDiffCache = new ConcurrentHashMap<>();
    this.fileDiffChanged = new ConcurrentHashMap<>();
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (!actionInfo.getActionName().equals("sync")) {
      return ScheduleResult.FAIL;
    }
    String srcDir = action.getArgs().get(SyncAction.SRC);
    String path = action.getArgs().get("-file");
    String destDir = action.getArgs().get(SyncAction.DEST);
    // Check again to avoid corner cases
    if (isFileLocked(path)) {
      return ScheduleResult.FAIL;
    }
    long fid = fileDiffChainMap.get(path).getHead();
    if (fid == -1) {
      // FileChain is already empty
      return ScheduleResult.FAIL;
    }
    // Lock this file/chain to avoid conflict
    fileLock.put(path, fid);
    FileDiff fileDiff = null;
    if (fileDiffCache.containsKey(fid)) {
      fileDiff = fileDiffCache.get(fid);
    } else {
      return ScheduleResult.FAIL;
    }

    if (fileDiff == null) {
      return ScheduleResult.FAIL;
    }
    if (fileDiff.getState() != FileDiffState.PENDING) {
      // If file diff is applied or failed
      fileDiffChainMap.get(path).removeHead();
      fileLock.remove(path);
      return ScheduleResult.FAIL;
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
  
  private boolean isFileLocked(String path) {
    if (fileLock.containsKey(path)) {
      // File is locked
      return true;
    }
    if (baseSyncQueue.containsKey(path)) {
      // File is in base sync queue
      return true;
    }
    if (!fileDiffChainMap.containsKey(path)) {
      // File Chain is not ready
      return true;
    }
    if (fileDiffChainMap.get(path).size() == 0) {
      // File Chain is empty
      return true;
    }
    return false;
  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) {
    String path = actionInfo.getArgs().get("-file");
    LOG.debug("Submit file {} with lock {}", path, fileLock.keySet());
    // If locked then false
    return !isFileLocked(path);
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
    // Remove lock
    FileDiff fileDiff = null;
    if (actionInfo.isFinished()) {
      try {
        long did = actionDiffMap.get(actionInfo.getActionId());
        // Remove for action diff map
        if (actionDiffMap.containsKey(actionInfo.getActionId())) {
          actionDiffMap.remove(actionInfo.getActionId());
        }
        if (fileDiffCache.containsKey(did)) {
          fileDiff = fileDiffCache.get(did);
        } else {
          LOG.error("Duplicate sync action->[ {} ] is triggered", did);
          return;
        }

        if (fileDiff == null) {
          return;
        }
        if (actionInfo.isSuccessful()) {
          if (fileDiffChainMap.containsKey(fileDiff.getSrc())) {
            // Remove from chain top
            fileDiffChainMap.get(fileDiff.getSrc()).removeHead();
          }
          //update state in cache
          updateFileDiffStatesInCache(did, FileDiffState.APPLIED);

          if (fileDiffMap.containsKey(did)) {
            fileDiffMap.remove(did);
          }

        } else {
          if (fileDiffMap.containsKey(did)) {
            int curr = fileDiffMap.get(did);
            if (curr >= retryTh) {
              //update state in cache
              updateFileDiffStatesInCache(did, FileDiffState.FAILED);
              directSync(fileDiff.getSrc(),
                  actionInfo.getArgs().get(SyncAction.SRC),
                  actionInfo.getArgs().get(SyncAction.DEST));
            } else {
              fileDiffMap.put(did, curr + 1);
            }
          }
        }
        if (fileLock.containsKey(fileDiff.getSrc())) {
          fileLock.remove(fileDiff.getSrc());
        }
      } catch (MetaStoreException e) {
        LOG.error("Mark sync action in metastore failed!", e);
      } catch (Exception e) {
        LOG.error("Sync action error", e);
      }
    }
  }

  private void batchDirectSync() throws MetaStoreException {
    int index = 0;
    // Use 90% of check interval to batchSync
    if (baseSyncQueue.size() == 0) {
      LOG.debug("Base Sync size = 0!");
      return;
    }
    List<FileDiff> batchFileDiffs = new ArrayList<>();
    List<String> removed = new ArrayList<>();
    FileDiff fileDiff;
    for (Iterator<Map.Entry<String, String>> it =
        baseSyncQueue.entrySet().iterator(); it.hasNext(); ) {
      if (index >= batchSize) {
        break;
      }
      Map.Entry<String, String> entry = it.next();
      fileDiff = directSync(entry.getKey(), entry.getValue());
      if (fileDiff != null) {
        batchFileDiffs.add(fileDiff);
      }
      removed.add(entry.getKey());
      index++;
    }
    // Batch Insert
    metaStore.insertFileDiffs(
        batchFileDiffs.toArray(new FileDiff[batchFileDiffs.size()]));
    // Remove from baseSyncQueue
    for (String src : removed) {
      baseSyncQueue.remove(src);
    }
  }

  FileStatus[] listFileStatuesOfDirs(String dirName) {
    FileSystem fs = null;
    FileStatus[] tmpFileStatus = null;
    List<FileStatus> returnStatus = new LinkedList<>();
    try {
      fs = FileSystem.get(URI.create(dirName), conf);
      tmpFileStatus = fs.listStatus(new Path(dirName));
      for (FileStatus fileStatus : tmpFileStatus) {
        if (!fileStatus.isDirectory()) {
          returnStatus.add(fileStatus);
        } else {
          //all the file in this fileStatuses
          FileStatus[] childFileStatuses = listFileStatuesOfDirs(fileStatus.getPath().getName());
          if (childFileStatuses != null && childFileStatuses.length != 0) {
            returnStatus.addAll(Arrays.asList(childFileStatuses));
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Fetch remote file list error!", e);
    }
    if (returnStatus.size() == 0) {
      return new FileStatus[0];
    }
    return returnStatus.toArray(new FileStatus[returnStatus.size()]);
  }

  private void baseSync(String srcDir,
      String destDir) throws MetaStoreException {
    List<FileInfo> srcFiles = metaStore.getFilesByPrefix(srcDir);
    LOG.debug("Directory Base Sync {} files", srcFiles.size());
    // <file name, fileInfo>
    Map<String, FileInfo> srcFileSet = new HashMap<>();
    for (FileInfo fileInfo : srcFiles) {
      // Remove prefix/parent
      srcFileSet.put(fileInfo.getPath().replace(srcDir, ""), fileInfo);
    }
    FileStatus[] fileStatuses = null;
    // recursively file lists
    fileStatuses = listFileStatuesOfDirs(destDir);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("Remote directory is empty!");
    } else {
      LOG.debug("Remote directory contains {} files!", fileStatuses.length);
      for (FileStatus fileStatus : fileStatuses) {
        // only get file name
        String destName = fileStatus.getPath().getName();
        if (srcFileSet.containsKey(destName)) {
          FileInfo fileInfo = srcFileSet.get(destName);
          String src = fileInfo.getPath();
          String dest = src.replace(srcDir, destDir);
          baseSyncQueue.put(src, dest);
          srcFileSet.remove(destName);
        }
      }
    }
    LOG.debug("Directory Base Sync {} files", srcFileSet.size());
    for (FileInfo fileInfo : srcFileSet.values()) {
      if (fileInfo.isdir()) {
        // Ignore directory
        continue;
      }
      String src = fileInfo.getPath();
      String dest = src.replace(srcDir, destDir);
      baseSyncQueue.put(src, dest);
      overwriteQueue.put(src, true);
      // directSync(src, dest);
    }
    batchDirectSync();
  }


  private void directSync(String src, String srcDir,
      String destDir) throws MetaStoreException {
    String dest = src.replace(srcDir, destDir);
    FileDiff fileDiff = directSync(src, dest);
    if (fileDiff != null) {
      metaStore.insertFileDiff(fileDiff);
    }
  }

  private FileDiff directSync(String src, String dest) throws MetaStoreException {
    FileInfo fileInfo = metaStore.getFile(src);
    if (fileInfo == null) {
      // Primary file doesn't exist
      return null;
    }
    // Mark all related diff as Merged
    if (fileDiffChainMap.containsKey(src)) {
      fileDiffChainMap.get(src).markAllDiffs();
      fileDiffChainMap.remove(src);
    }
    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName(src);
    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getState() == FileDiffState.PENDING) {
        metaStore.updateFileDiff(fileDiff.getDiffId(), FileDiffState.MERGED);
      }
    }
    FileDiff fileDiff;
    long offSet;
    if (overwriteQueue.containsKey(src)) {
      offSet = 0;
      overwriteQueue.remove(src);
    } else {
      offSet = fileCompare(fileInfo, dest);
    }
    if (offSet == fileInfo.getLength()) {
      LOG.debug("Primary len={}, remote len={}", fileInfo.getLength(), offSet);
      return null;
    } else if (offSet > fileInfo.getLength()) {
      // Remove dirty remote file
      fileDiff = new FileDiff(FileDiffType.DELETE, FileDiffState.PENDING);
      fileDiff.setSrc(src);
      metaStore.insertFileDiff(fileDiff);
      offSet = 0;
    }
    // Copy tails to remote
    fileDiff = new FileDiff(FileDiffType.APPEND, FileDiffState.PENDING);
    fileDiff.setSrc(src);
    // Append changes to remote files
    fileDiff.getParameters()
        .put("-length", String.valueOf(fileInfo.getLength() - offSet));
    fileDiff.getParameters().put("-offset", String.valueOf(offSet));
    fileDiff.setRuleId(-1);
    return fileDiff;
  }

  private long fileCompare(FileInfo fileInfo,
      String dest) throws MetaStoreException {
    // Primary
    long localLen = fileInfo.getLength();
    // Get InputStream from URL
    FileSystem fs = null;
    // Get file statue from remote HDFS
    try {
      fs = FileSystem.get(URI.create(dest), conf);
      FileStatus fileStatus = fs.getFileStatus(new Path(dest));
      long remoteLen = fileStatus.getLen();
      // TODO Add Checksum check
      // Remote
      if (localLen == remoteLen) {
        return localLen;
      } else {
        return remoteLen;
      }
    } catch (IOException e) {
      return 0;
    }
  }

  // add file_diff into Cache, if the file diff is already in cache, we update
  // if the file diff is not in cache, we insert.
  public void addDiffIntoCache(
      List<FileDiff> fileDiffs) throws MetaStoreException {
    LOG.debug("Add FileDiff Cache into file_diff cache");
    for (FileDiff fileDiff : fileDiffs) {
      recordChangedFileDiff(fileDiff);
      fileDiffCache.put(fileDiff.getDiffId(), fileDiff);
    }
    if (fileDiffChanged.size() >= cacheSyncTh) {
      // update
      pushCacheIntoDB();
    }
  }

  // add file_diff into Cache, if the file diff is already in cache, we update
  // if the file diff is not in cache, we insert.
  public void addDiffIntoCache(FileDiff fileDiff) throws MetaStoreException {
    LOG.debug("Add FileDiff Cache into file_diff cache");
    recordChangedFileDiff(fileDiff);
    fileDiffCache.put(fileDiff.getDiffId(), fileDiff);
    if (fileDiffChanged.size() >= cacheSyncTh) {
      // update
      pushCacheIntoDB();
    }
  }

  public void recordChangedFileDiff(FileDiff fileDiff) {
    //judge whether change the file diff
    if (fileDiffCache.containsKey(fileDiff.getDiffId())) {
      FileDiff oldDiff = fileDiffCache.get(fileDiff.getDiffId());
      if (oldDiff != fileDiff) {
        fileDiffChanged.put(fileDiff.getDiffId(), true);
      }
    } else {
      //this is the new file_diff inserted into file_diff cache
      //init the fileDiffchanged map
      fileDiffChanged.put(fileDiff.getDiffId(), false);
    }
  }

  public void deleteDiffInCache(Long did) {
    LOG.debug("Delete FileDiff in cache");
    if (fileDiffCache.containsKey(did)) {
      fileDiffCache.remove(did);
      fileDiffChanged.remove(did);
    }
  }

  public void updateFileDiffStatesInCache(Long did, FileDiffState fileDiffState) {
    LOG.debug("Update FileDiff");
    if (!fileDiffCache.containsKey(did)) {
      return;
    }
    FileDiff fileDiff = fileDiffCache.get(did);
    fileDiff.setState(fileDiffState);
    //update
    fileDiffChanged.put(did, true);
    fileDiffCache.put(did, fileDiff);
  }

  public void pushCacheIntoDB() throws MetaStoreException {
    LOG.debug("Push FileFiff From cache Into FileDiff");
    List<Long> dids = new ArrayList<>();
    List<FileDiffState> states = new ArrayList<>();
    List<String> param = new ArrayList<>();
    List<Long> needDel = new ArrayList<>();
    for (Long key : fileDiffCache.keySet()) {
      FileDiff fileDiff;
      if (fileDiffChanged.get(key)) {
        fileDiff = fileDiffCache.get(key);
        dids.add(fileDiff.getDiffId());
        states.add(fileDiff.getState());
        param.add(fileDiff.getParametersJsonString());
        // TODO need to be confirmed what should be delete
        if (!(fileDiff.getState() == FileDiffState.PENDING || fileDiff.getState() == FileDiffState.RUNNING)) {
          needDel.add(key);
        }
      }
    }
    //update to DB
    if (dids.size() > 0) {
      metaStore.batchUpdateFileDiff(dids, states, param);
      for (long did : needDel) {
        deleteDiffInCache(did);
      }
    }
  }

  @Override
  public void init() throws IOException {
  }

  @Override
  public void start() throws IOException {
    executorService.scheduleAtFixedRate(
        new CopyScheduler.ScheduleTask(), 0, checkInterval,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    executorService.shutdown();
  }

  // private void lockFile(String fileName, long did) {
  //   fileLock.put(fileName, did);
  // }
  //
  // private void unlockFile(String fileName) {
  //   if (fileLock.containsKey(fileName)) {
  //     fileLock.remove(fileName);
  //   }
  // }

  private class ScheduleTask implements Runnable {

    private void syncFileDiff() {
      List<FileDiff> pendingDiffs = null;
      try {
        pushCacheIntoDB();
        pendingDiffs = metaStore.getPendingDiff();
        diffPreProcessing(pendingDiffs);
      } catch (MetaStoreException e) {
        LOG.error("Sync fileDiffs error", e);
      }
    }

    private void diffPreProcessing(
        List<FileDiff> fileDiffs) throws MetaStoreException {
      // Merge all existing fileDiffs into fileChains
      LOG.debug("Size of Pending diffs {}", fileDiffs.size());
      if (fileDiffs.size() == 0 && baseSyncQueue.size() == 0) {
        LOG.debug("All Backup directories are synced");
        return;
      }
      for (FileDiff fileDiff : fileDiffs) {
        if (fileDiff.getDiffType() == FileDiffType.BASESYNC) {
          metaStore.updateFileDiff(fileDiff.getDiffId(), FileDiffState.MERGED);
          baseSync(fileDiff.getSrc(), fileDiff.getParameters().get("-dest"));
          return;
        }
        FileChain fileChain;
        String src = fileDiff.getSrc();
        // Skip applying file diffs
        if (fileDiffMap.containsKey(fileDiff.getDiffId())) {
          continue;
        }
        if (baseSyncQueue.containsKey(fileDiff.getSrc())) {
          // Will be directly sync
          continue;
        }
        // Get or create fileChain
        if (fileDiffChainMap.containsKey(src)) {
          fileChain = fileDiffChainMap.get(src);
        } else {
          fileChain = new FileChain(src);
          fileDiffChainMap.put(src, fileChain);
        }
        fileChain.addToChain(fileDiff);
        fileDiffMap.put(fileDiff.getDiffId(), 0);
      }
    }

    @Override
    public void run() {
      try {
        batchDirectSync();
        syncFileDiff();
        // addToRunning();
      } catch (Exception e) {
        LOG.error("CopyScheduler Run Error", e);
      }
    }

    private class FileChain {
      // Current append length in chain
      private long currAppendLength;
      // Current file path/name
      private String filePath;
      // file diff id
      private List<Long> diffChain;
      // append file diff id
      private List<Long> appendChain;
      // file name change trace
      private List<String> nameChain;

      FileChain() {
        this.diffChain = new ArrayList<>();
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

      public List<Long> getDiffChain() {
        return diffChain;
      }

      public void setDiffChain(List<Long> diffChain) {
        this.diffChain = diffChain;
      }

      public int size() {
        return diffChain.size();
      }

      public void addToChain(FileDiff fileDiff) throws MetaStoreException {
        addDiffIntoCache(fileDiff);
        long did = fileDiff.getDiffId();
        if (fileDiff.getDiffType() == FileDiffType.APPEND) {
          if (currAppendLength >= mergeLenTh ||
              appendChain.size() >= mergeCountTh) {
            mergeAppend();
          }
          // Add Append to Append Chain
          appendChain.add(did);
          // Increase Append length
          currAppendLength +=
              Long.valueOf(fileDiff.getParameters().get("-length"));
          diffChain.add(did);
        } else if (fileDiff.getDiffType() == FileDiffType.RENAME) {
          // Add New Name to Name Chain
          mergeRename(fileDiff);
        } else if (fileDiff.getDiffType() == FileDiffType.DELETE) {
          mergeDelete(fileDiff);
        } else {
          // Metadata
          diffChain.add(did);
        }
      }

      @VisibleForTesting
      void mergeAppend() throws MetaStoreException {
        if (fileLock.containsKey(filePath)) {
          return;
        }
        LOG.debug("Append Merge Triggered!");
        // Lock file to avoid File Chain being processed
        fileLock.put(filePath, -1L);
        long offset = Integer.MAX_VALUE;
        long totalLength = 0;
        long lastAppend = -1;
        for (long did : appendChain) {
          FileDiff fileDiff = fileDiffCache.get(did);
          if (fileDiff != null && fileDiff.getState().getValue() != 2) {
            long currOffset =
                Long.valueOf(fileDiff.getParameters().get("-offset"));
            if (offset > currOffset) {
              offset = currOffset;
            }
            if (currOffset != offset && currOffset != totalLength + offset) {
              // offset and length check to avoid dirty append
              break;
            }
            metaStore.updateFileDiff(did, FileDiffState.APPLIED);
            // Add current file length to length
            totalLength +=
                Long.valueOf(fileDiff.getParameters().get("-length"));
            lastAppend = did;
          }
        }
        if (lastAppend == -1) {
          return;
        }
        FileDiff fileDiff = fileDiffCache.get(lastAppend);
        fileDiff.getParameters().put("-offset", "" + offset);
        fileDiff.getParameters().put("-length", "" + totalLength);
        // Update fileDiff in metastore
        metaStore.updateFileDiff(fileDiff.getDiffId(),
            FileDiffState.PENDING, fileDiff.getParametersJsonString());
        // Unlock file
        fileLock.remove(filePath);
        currAppendLength = 0;
        appendChain.clear();
      }

      @VisibleForTesting
      void mergeDelete(FileDiff fileDiff) throws MetaStoreException {
        LOG.debug("Delete Merge Triggered!");
        boolean isCreate = false;
        for (long did : appendChain) {
          FileDiff diff = fileDiffCache.get(did);
          if (diff.getParameters().containsKey("-offset")) {
            if (diff.getParameters().get("-offset").equals("0")) {
              isCreate = true;
            }
          }
          metaStore.updateFileDiff(did, FileDiffState.APPLIED);
        }
        appendChain.clear();
        if (!isCreate) {
          if (nameChain.size() > 1) {
            fileDiff.setSrc(nameChain.get(0));
            // Delete raw is enough
            metaStore.updateFileDiff(fileDiff.getDiffId(), nameChain.get(0));
          }
          diffChain.add(fileDiff.getDiffId());
        } else {
          metaStore.updateFileDiff(fileDiff.getDiffId(), FileDiffState.APPLIED);
        }
      }

      @VisibleForTesting
      void mergeChain(FileChain previousChain) {
      }

      @VisibleForTesting
      void mergeMeta() {
        if (fileLock.containsKey(filePath)) {
          return;
        }
      }

      @VisibleForTesting
      void mergeRename(FileDiff fileDiff) throws MetaStoreException {
        // Rename action will effect all append actions
        if (fileLock.containsKey(filePath)) {
          return;
        }
        LOG.debug("Rename Merge Triggered!");
        // Lock file to avoid File Chain being processed
        fileLock.put(filePath, -1L);
        String newName = fileDiff.getParameters().get("-dest");
        nameChain.add(newName);
        boolean isCreate = false;
        for (long did : appendChain) {
          FileDiff appendFileDiff = fileDiffCache.get(did);
          if (fileDiff.getParameters().containsKey("-offset")) {
            if (fileDiff.getParameters().get("-offset").equals("0")) {
              isCreate = true;
            }
          }
          if (appendFileDiff != null &&
              appendFileDiff.getState().getValue() != 2) {
            appendFileDiff.setSrc(newName);
            metaStore.updateFileDiff(did, newName);
          }
        }
        // Insert rename fileDiff to head
        if (!isCreate) {
          diffChain.add(0, fileDiff.getDiffId());
        } else {
          metaStore.updateFileDiff(fileDiff.getDiffId(), FileDiffState.APPLIED);
        }
        // Unlock file
        fileLock.remove(filePath);
      }

      public long getHead() {
        if (diffChain.size() == 0) {
          return -1;
        }
        return diffChain.get(0);
      }

      public long removeHead() {
        if (diffChain.size() == 0) {
          return -1;
        }
        long fid = diffChain.get(0);
        if (appendChain.size() > 0 && fid == appendChain.get(0)) {
          appendChain.remove(0);
        }
        diffChain.remove(0);
        if (diffChain.size() == 0) {
          fileDiffChainMap.remove(filePath);
        }
        return fid;
      }

      public void markAllDiffs() throws MetaStoreException {
        for (long did : diffChain) {
          metaStore.updateFileDiff(did, FileDiffState.MERGED);
        }
        diffChain.clear();
      }
    }
  }
}
