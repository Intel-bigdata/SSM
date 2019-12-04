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
import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.action.SyncAction;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.*;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.LaunchCmdlet;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CopyScheduler extends ActionSchedulerService {
  static final Logger LOG =
      LoggerFactory.getLogger(CopyScheduler.class);
  private static final List<String> actions = Collections.singletonList("sync");
  private MetaStore metaStore;

  // Fixed rate scheduler
  private ScheduledExecutorService executorService;
  // Global variables
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
  private long checkInterval;
  // Base sync batch insert size
  private int batchSize = 500;
  // Cache of the file_diff
  private Map<Long, FileDiff> fileDiffCache;
  // cache sync threshold, default 100
  private int cacheSyncTh = 100;
  // record the file_diff whether being changed
  private Map<Long, Boolean> fileDiffCacheChanged;
  // throttle for copy action
  private long throttleInMb;
  private RateLimiter rateLimiter = null;
  // records the number of file diffs in useless states
  private AtomicInteger numFileDiffUseless = new AtomicInteger(0);
  // record the file diff info in order for check use
  private List<FileDiff> fileDiffArchive;
  public static final int fileDiffArchiveSize = 1000;

  public CopyScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.fileLock = new ConcurrentHashMap<>();
    this.actionDiffMap = new ConcurrentHashMap<>();
    this.fileDiffChainMap = new ConcurrentHashMap<>();
    this.fileDiffMap = new ConcurrentHashMap<>();
    this.baseSyncQueue = new ConcurrentHashMap<>();
    this.overwriteQueue = new ConcurrentHashMap<>();
    this.executorService = Executors.newScheduledThreadPool(2);
    this.fileDiffCache = new ConcurrentHashMap<>();
    this.fileDiffCacheChanged = new ConcurrentHashMap<>();
    // Get conf or new default conf
    try {
      conf = getContext().getConf();
    } catch (NullPointerException e) {
      // SmartContext is empty
      conf = new Configuration();
    }
    // Conf related parameters
    cacheSyncTh = conf.getInt(SmartConfKeys
            .SMART_COPY_SCHEDULER_BASE_SYNC_BATCH,
        SmartConfKeys.SMART_COPY_SCHEDULER_BASE_SYNC_BATCH_DEFAULT);
    checkInterval = conf.getLong(SmartConfKeys.SMART_COPY_SCHEDULER_CHECK_INTERVAL,
        SmartConfKeys.SMART_COPY_SCHEDULER_CHECK_INTERVAL_DEFAULT);
    throttleInMb = conf.getLong(SmartConfKeys.SMART_ACTION_COPY_THROTTLE_MB_KEY,
        SmartConfKeys.SMART_ACTION_COPY_THROTTLE_MB_DEFAULT);
    if (throttleInMb > 0) {
      rateLimiter = RateLimiter.create(throttleInMb);
    }
    try {
      this.numFileDiffUseless.addAndGet(metaStore.getUselessFileDiffNum());
    } catch (MetaStoreException e) {
      LOG.error("Failed to get num of useless file diffs!");
    }
    this.fileDiffArchive = new CopyOnWriteArrayList<>();
  }

  @Override
  public ScheduleResult onSchedule(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      LaunchCmdlet cmdlet, LaunchAction action, int actionIndex) {
    if (!actionInfo.getActionName().equals("sync")) {
      return ScheduleResult.FAIL;
    }
    String srcDir = action.getArgs().get(SyncAction.SRC);
    String path = action.getArgs().get(HdfsAction.FILE_PATH);
    String destDir = action.getArgs().get(SyncAction.DEST);
    String destPath = path.replaceFirst(srcDir, destDir);
    // Check again to avoid corner cases
    long did = fileDiffChainMap.get(path).getHead();
    if (did == -1) {
      // FileChain is already empty
      return ScheduleResult.FAIL;
    }
    FileDiff fileDiff = fileDiffCache.get(did);
    if (fileDiff == null) {
      return ScheduleResult.FAIL;
    }
    if (fileDiff.getState() != FileDiffState.PENDING) {
      // If file diff is applied or failed
      fileDiffChainMap.get(path).removeHead();
      fileLock.remove(path);
      return ScheduleResult.FAIL;
    }
    // wait dependent file diff
    if (requireWait(fileDiff)) {
      return ScheduleResult.RETRY;
    }

    // Check whether src is compressed, if so, the syncing length should be original length.
    // Otherwise, only partial compressed file is copied. And the dest file is not compressed.
    // Using HDFS copy cmd or SSM copy action will not have such issue, since file length
    // is obtained from SmartDFSClient in that case, where original length is acquired.
    try {
      FileState fileState = metaStore.getFileState(fileDiff.getSrc());
      if (fileState instanceof CompressionFileState &&
          fileDiff.getParameters().get("-length") != null) {
        Long length = ((CompressionFileState) fileState).getOriginalLength();
        fileDiff.getParameters().put("-length", length.toString());
      }
    } catch (MetaStoreException e) {
      LOG.error("Failed to get FileState, the syncing file's length may be " +
          "incorrect if it is compressed", e);
    }

    switch (fileDiff.getDiffType()) {
      case APPEND:
        action.setActionType("copy");
        action.getArgs().put("-dest", destPath);
        if (rateLimiter != null) {
          String strLen = fileDiff.getParameters().get("-length");
          if (strLen != null) {
            int appendLen = (int)(Long.valueOf(strLen) >> 20);
            if (appendLen > 0) {
              if (!rateLimiter.tryAcquire(appendLen)) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Cancel Scheduling COPY action {} due to throttling.", actionInfo);
                }
                return ScheduleResult.RETRY;
              }
            }
          }
        }
        break;
      case DELETE:
        action.setActionType("delete");
        action.getArgs().put(HdfsAction.FILE_PATH, destPath);
        break;
      case RENAME:
        action.setActionType("rename");
        action.getArgs().put(HdfsAction.FILE_PATH, destPath);
        // TODO scope check
        String remoteDest = fileDiff.getParameters().get("-dest");
        action.getArgs().put("-dest", remoteDest.replaceFirst(srcDir, destDir));
        fileDiff.getParameters().remove("-dest");
        break;
      case METADATA:
        action.setActionType("metadata");
        action.getArgs().put(HdfsAction.FILE_PATH, destPath);
        break;
      default:
        break;
    }
    // Put all parameters into args
    action.getArgs().putAll(fileDiff.getParameters());
    actionDiffMap.put(actionInfo.getActionId(), did);
    if (!fileDiffMap.containsKey(did)) {
      fileDiffMap.put(did, 1);
    }
    return ScheduleResult.SUCCESS;
  }

  @Override
  public List<String> getSupportedActions() {
    return actions;
  }
  
  private boolean isFileLocked(String path) {
    if(fileLock.size() == 0) {
      LOG.debug("File Lock is empty. Current path = {}", path);
    }
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

  public boolean requireWait(FileDiff fileDiff) {
    for (FileDiff archiveDiff : fileDiffArchive) {
      if (fileDiff.getDiffId() == archiveDiff.getDiffId()) {
        break;
      }
      if (!FileDiffState.isTerminalState(archiveDiff.getState())) {
        String fileDiffPath = fileDiff.getSrc().endsWith("/") ?
            fileDiff.getSrc() : fileDiff.getSrc() + "/";
        String archiveDiffPath = archiveDiff.getSrc().endsWith("/") ?
            archiveDiff.getSrc() : archiveDiff.getSrc() + "/";
        if (fileDiffPath.startsWith(archiveDiffPath) || archiveDiffPath.startsWith(fileDiffPath)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean onSubmit(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex)
      throws IOException {
    // check args
    if (actionInfo.getArgs() == null) {
      throw new IOException("No arguments for the action");
    }
    String path = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    LOG.debug("Submit file {} with lock {}", path, fileLock.keySet());
    // If locked then false
    if (!isFileLocked(path)) {
      // Lock this file/chain to avoid conflict
      fileLock.put(path, 0L);
      return true;
    }
    throw new IOException("The submit file " + path + " is in use by another program or user");
  }

  @Override
  public void onActionFinished(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
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
          fileDiffTerminated(fileDiff);
          //update state in cache
          updateFileDiffInCache(did, FileDiffState.APPLIED);
        } else {
          if (fileDiffMap.containsKey(did)) {
            int curr = fileDiffMap.get(did);
            if (curr >= retryTh) {
              fileDiffTerminated(fileDiff);
              //update state in cache
              updateFileDiffInCache(did, FileDiffState.FAILED);
            } else {
              fileDiffMap.put(did, curr + 1);
              // Unlock this file for retry
              fileLock.remove(fileDiff.getSrc());
            }
          } else {
            fileDiffTerminated(fileDiff);
            updateFileDiffInCache(did, FileDiffState.FAILED);
          }
        }
      } catch (MetaStoreException e) {
        LOG.error("Mark sync action in metastore failed!", e);
      } catch (Exception e) {
        LOG.error("Sync action error", e);
      }
    }
  }

  public void fileDiffTerminated(FileDiff fileDiff) {
    if (fileDiffChainMap.containsKey(fileDiff.getSrc())) {
      // Remove chain top
      fileDiffChainMap.get(fileDiff.getSrc()).removeHead();
    }
    // remove from fileDiffMap which is for retry use
    if (fileDiffMap.containsKey(fileDiff.getDiffId())) {
      fileDiffMap.remove(fileDiff.getDiffId());
    }
  }

  public void fileDiffTerminatedInternal(FileDiff fileDiff) {
    if (fileDiffChainMap.containsKey(fileDiff.getSrc())) {
      // Remove the fileDiff from chain
      fileDiffChainMap.get(fileDiff.getSrc()).removeFromChain(fileDiff);
    }
    // remove from fileDiffMap which is for retry use
    if (fileDiffMap.containsKey(fileDiff.getDiffId())) {
      fileDiffMap.remove(fileDiff.getDiffId());
    }
  }

  private void batchDirectSync() throws MetaStoreException {
    // Use 90% of check interval to batchSync
    if (baseSyncQueue.size() == 0) {
      return;
    }
    LOG.debug("Base Sync size = {}", baseSyncQueue.size());
    List<FileDiff> batchFileDiffs = new ArrayList<>();
    List<String> removed = new ArrayList<>();
    FileDiff fileDiff;
    int index = 0;
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
    Long dids[] = metaStore.insertFileDiffs(batchFileDiffs);
    for (int i = 0; i < dids.length; i++) {
      batchFileDiffs.get(i).setDiffId(dids[i]);
    }
    fileDiffArchive.addAll(batchFileDiffs);
    // Remove from baseSyncQueue
    for (String src : removed) {
      baseSyncQueue.remove(src);
    }
  }

  private FileStatus[] listFileStatuesOfDirs(String dirName) {
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
          if (childFileStatuses.length != 0) {
            returnStatus.addAll(Arrays.asList(childFileStatuses));
          }
        }
      }
    } catch (IOException e) {
      LOG.debug("Fetch remote file list error!", e);
    }
    if (returnStatus.size() == 0) {
      return new FileStatus[0];
    }
    return returnStatus.toArray(new FileStatus[returnStatus.size()]);
  }

  private void baseSync(String srcDir,
      String destDir) throws MetaStoreException {
    List<FileInfo> srcFiles = metaStore.getFilesByPrefix(srcDir);
    if (srcFiles.size() > 0) {
      LOG.info("Directory Base Sync {} files", srcFiles.size());
    }
    // <file name, fileInfo>
    Map<String, FileInfo> srcFileSet = new HashMap<>();
    for (FileInfo fileInfo : srcFiles) {
      // Remove prefix/parent
      srcFileSet.put(fileInfo.getPath().replaceFirst(srcDir, ""), fileInfo);
    }
    FileStatus[] fileStatuses = null;
    // recursively file lists
    fileStatuses = listFileStatuesOfDirs(destDir);
    if (fileStatuses.length == 0) {
      LOG.debug("Remote directory is empty!");
    } else {
      LOG.debug("Remote directory contains {} files!", fileStatuses.length);
      for (FileStatus fileStatus : fileStatuses) {
        // only get file name
        String destName = fileStatus.getPath().getName();
        if (srcFileSet.containsKey(destName)) {
          FileInfo fileInfo = srcFileSet.get(destName);
          String src = fileInfo.getPath();
          String dest = src.replaceFirst(srcDir, destDir);
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
      String dest = src.replaceFirst(srcDir, destDir);
      baseSyncQueue.put(src, dest);
      overwriteQueue.put(src, true);
      // directSync(src, dest);
    }
    batchDirectSync();
  }

  private FileDiff directSync(String src, String dest) throws MetaStoreException {
    FileInfo fileInfo = metaStore.getFile(src);
    if (fileInfo == null) {
      // Primary file doesn't exist
      return null;
    }
    if (fileLock.containsKey(src)) {
      // File is syncing
      return null;
    }
    // Lock file to avoid diff apply
    fileLock.put(src, 0L);
    // Mark all related diff in cache as Merged
    if (fileDiffChainMap.containsKey(src)) {
      fileDiffChainMap.get(src).markAllDiffs();
      fileDiffChainMap.remove(src);
      pushCacheToDB();
    }
    // Mark all related diff in metastore as Merged
    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName(src);
    List<Long> dids = new ArrayList<>();
    for (FileDiff fileDiff : fileDiffs) {
      if (fileDiff.getState() == FileDiffState.PENDING) {
        dids.add(fileDiff.getDiffId());
      }
    }
    metaStore.batchUpdateFileDiff(dids, FileDiffState.MERGED);
    for (long did : dids) {
      updateFileDiffArchive(did, FileDiffState.MERGED);
    }
    // Unlock this file
    fileLock.remove(src);
    // Generate a new file diff
    FileDiff fileDiff;
    long offSet;
    if (overwriteQueue.containsKey(src)) {
      offSet = -1;
      overwriteQueue.remove(src);
    } else {
      offSet = fileCompare(fileInfo, dest);
    }
    if (offSet == -1) {
      // Remote file does not exist
      offSet = 0;
    } else if (offSet == fileInfo.getLength()) {
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
      return remoteLen;
    } catch (IOException e) {
      return -1;
    }
  }

  /***
   * add fileDiff to Cache, if diff is already in cache, then print error log
   * @param fileDiff
   * @throws MetaStoreException
   */
  private void addDiffToCache(FileDiff fileDiff) throws MetaStoreException {
    LOG.debug("Add FileDiff Cache into file_diff cache");
    if (fileDiffCache.containsKey(fileDiff.getDiffId())) {
      LOG.error("FileDiff {} already in cache!", fileDiff);
      return;
    }
    fileDiffCache.put(fileDiff.getDiffId(), fileDiff);
  }

  private synchronized void updateFileDiffInCache(Long did,
      FileDiffState fileDiffState) throws MetaStoreException {
    LOG.debug("Update FileDiff");
    if (!fileDiffCache.containsKey(did)) {
      return;
    }
    FileDiff fileDiff = fileDiffCache.get(did);
    fileDiff.setState(fileDiffState);
    // Update
    fileDiffCacheChanged.put(did, true);
    fileDiffCache.put(did, fileDiff);
    updateFileDiffArchive(did, fileDiffState);
    if (fileDiffCacheChanged.size() >= cacheSyncTh) {
      // update
      pushCacheToDB();
    }
    if (FileDiffState.isUselessFileDiff(fileDiffState)) {
      numFileDiffUseless.incrementAndGet();
    }
  }

  private synchronized void updateFileDiffArchive(long did, FileDiffState state) {
    for (FileDiff diff : fileDiffArchive) {
      if (diff.getDiffId() == did) {
        diff.setState(state);
      }
    }
  }

  /***
   * delete cache and remove file lock if necessary
   * @param did
   */
  private void deleteDiffInCache(Long did) {
    LOG.debug("Delete FileDiff in cache");
    if (fileDiffCache.containsKey(did)) {
      FileDiff fileDiff = fileDiffCache.get(did);
      fileDiffCache.remove(did);
      fileDiffCacheChanged.remove(did);
      // Remove file lock
      if (fileLock.containsKey(fileDiff.getSrc())) {
        fileLock.remove(fileDiff.getSrc());
      }
    }
  }

  private synchronized void pushCacheToDB() throws MetaStoreException {
    List<FileDiff> updatedFileDiffs = new ArrayList<>();
    List<Long> needDel = new ArrayList<>();
    FileDiff fileDiff;
    // Only check changed cache rather than full cache
    for (Long did: fileDiffCacheChanged.keySet()) {
      fileDiff = fileDiffCache.get(did);
      if (fileDiff == null) {
        needDel.add(did);
        continue;
      }
      updatedFileDiffs.add(fileDiff);
      if (FileDiffState.isTerminalState(fileDiff.getState())) {
        needDel.add(did);
      }
    }
    // Push cache to metastore
    if (updatedFileDiffs.size() != 0) {
      LOG.debug("Push FileDiff from cache to metastore");
      metaStore.updateFileDiff(updatedFileDiffs);
    }
    // Remove file diffs in cache and file lock
    for (long did : needDel) {
      deleteDiffInCache(did);
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
    // The PurgeFileDiffTask runs in the period of 1800s
    executorService.scheduleAtFixedRate(
        new PurgeFileDiffTask(conf), 0, 1800, TimeUnit.SECONDS);
  }

  @Override
  public void stop() throws IOException {
    try {
      batchDirectSync();
    } catch (MetaStoreException e) {
      throw new IOException(e);
    }
    executorService.shutdown();
  }

  private boolean fileExistOnStandby(String filePath) {
    // TODO Need to be more general to handle failure
    try {
      // Check if file exists at standby cluster
      FileSystem fs = FileSystem.get(URI.create(filePath), conf);
      return fs.exists(new Path(filePath));
    } catch (IOException e) {
      LOG.debug("Fetch remote file status fails!", e);
      return false;
    }
  }

  private class ScheduleTask implements Runnable {

    private void syncFileDiff() {
      List<FileDiff> pendingDiffs = null;
      try {
        pushCacheToDB();
        pendingDiffs = metaStore.getPendingDiff();
        diffPreProcessing(pendingDiffs);
      } catch (MetaStoreException e) {
        LOG.error("Sync fileDiffs error", e);
      }
    }

    private void diffPreProcessing(
        List<FileDiff> fileDiffs) throws MetaStoreException {
      for (FileDiff fileDiff: fileDiffs) {
        addToFileDiffArchive(fileDiff);
      }
      // Merge all existing fileDiffs into fileChains
      LOG.debug("Size of Pending diffs {}", fileDiffs.size());
      if (fileDiffs.size() == 0 && baseSyncQueue.size() == 0) {
        LOG.debug("All Backup directories are synced");
        return;
      }
      for (FileDiff fileDiff : fileDiffs) {
        if (fileDiff.getDiffType() == FileDiffType.BASESYNC) {
          metaStore.updateFileDiff(fileDiff.getDiffId(), FileDiffState.MERGED);
          updateFileDiffArchive(fileDiff.getDiffId(), FileDiffState.MERGED);
          baseSync(fileDiff.getSrc(), fileDiff.getParameters().get("-dest"));
          return;
        }
        FileChain fileChain;
        String src = fileDiff.getSrc();
        // Skip diff in cache
        if (fileDiffCache.containsKey(fileDiff.getDiffId())) {
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
      }
    }

    private void addToFileDiffArchive(FileDiff newFileDiff) {
      for (FileDiff fileDiff: fileDiffArchive) {
        if (fileDiff.getDiffId() == newFileDiff.getDiffId()) {
          return;
        }
      }
      fileDiffArchive.add(newFileDiff);
      int index = 0;
      while (fileDiffArchive.size() > fileDiffArchiveSize && index < fileDiffArchiveSize) {
        if (FileDiffState.isTerminalState(fileDiffArchive.get(index).getState())) {
          fileDiffArchive.remove(index);
          continue;
        }
        index++;
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

      FileChain(String filePath) {
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

      void addToChain(FileDiff fileDiff) throws MetaStoreException {
        addDiffToCache(fileDiff);
        long did = fileDiff.getDiffId();
        if (fileDiff.getDiffType() == FileDiffType.APPEND) {
          String offset = fileDiff.getParameters().get("-offset");
          if (offset != null && offset.equals("0") && diffChain.size() != 0) {
            markAllDiffs();
          }

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
          if (isRenameSyncedFile(fileDiff)) {
            // Add New Name to Name Chain
            mergeRename(fileDiff);
          } else {
            fileDiffTerminatedInternal(fileDiff);
            // discard rename file diff due to not synced
            updateFileDiffInCache(fileDiff.getDiffId(), FileDiffState.FAILED);
            discardDirtyData(fileDiff);
          }
        } else if (fileDiff.getDiffType() == FileDiffType.DELETE) {
          mergeDelete(fileDiff);
        } else {
          // Metadata
          diffChain.add(did);
        }
      }

      void discardDirtyData(FileDiff fileDiff) throws MetaStoreException {
        // Clean dirty data
        List<BackUpInfo> backUpInfos = metaStore.getBackUpInfoBySrc(fileDiff.getSrc());
        for (BackUpInfo backUpInfo : backUpInfos) {
          FileDiff deleteFileDiff = new FileDiff(FileDiffType.DELETE, FileDiffState.PENDING);
          // use the rename file diff's src as delete file diff src
          deleteFileDiff.setSrc(fileDiff.getSrc());
          String destPath = deleteFileDiff.getSrc().replaceFirst(backUpInfo.getSrc(), backUpInfo.getDest());
          //put sync's dest path in parameter for delete use
          deleteFileDiff.getParameters().put("-dest", destPath);
          long did = metaStore.insertFileDiff(deleteFileDiff);
          deleteFileDiff.setDiffId(did);
          fileDiffArchive.add(deleteFileDiff);
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
          if (fileDiff != null && fileDiff.getState() != FileDiffState.APPLIED) {
            long currOffset =
                Long.valueOf(fileDiff.getParameters().get("-offset"));
            if (offset > currOffset) {
              offset = currOffset;
            }
            if (currOffset != offset && currOffset != totalLength + offset) {
              // Check offset and length to avoid dirty append
              break;
            }
            updateFileDiffInCache(did, FileDiffState.APPLIED);
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
        fileDiffCacheChanged.put(fileDiff.getDiffId(), true);
        // Unlock file
        fileLock.remove(filePath);
        currAppendLength = 0;
        appendChain.clear();
      }

      @VisibleForTesting
      void mergeDelete(FileDiff fileDiff) throws MetaStoreException {
//        LOG.debug("Delete Merge Triggered!");
//        for (long did : appendChain) {
//          FileDiff diff = fileDiffCache.get(did);
//          fileDiffTerminatedInternal(diff);
//          updateFileDiffInCache(did, FileDiffState.APPLIED);
//        }
//        appendChain.clear();
        for (FileDiff archiveDiff : fileDiffArchive) {
          if (archiveDiff.getDiffId() == fileDiff.getDiffId()) {
            break;
          }
          if (FileDiffState.isTerminalState(archiveDiff.getState())) {
            continue;
          }
          String fileDiffPath = fileDiff.getSrc().endsWith("/") ?
              fileDiff.getSrc() : fileDiff.getSrc() + "/";
          String archiveDiffPath = archiveDiff.getSrc().endsWith("/") ?
              archiveDiff.getSrc() : archiveDiff.getSrc() + "/";
          if (archiveDiffPath.startsWith(fileDiffPath)) {
            fileDiffTerminatedInternal(archiveDiff);
            updateFileDiffInCache(archiveDiff.getDiffId(), FileDiffState.APPLIED);
          }
        }
        diffChain.add(fileDiff.getDiffId());
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
              appendFileDiff.getState() != FileDiffState.APPLIED) {
            appendFileDiff.setSrc(newName);
            fileDiffCacheChanged.put(appendFileDiff.getDiffId(), true);
          }
        }
        // Insert rename fileDiff to head
        if (!isCreate) {
          diffChain.add(0, fileDiff.getDiffId());
        } else {
          updateFileDiffInCache(fileDiff.getDiffId(), FileDiffState.APPLIED);
        }
        // Unlock file
        fileLock.remove(filePath);
      }

      boolean isRenameSyncedFile(FileDiff renameFileDiff) throws MetaStoreException {
        String path = renameFileDiff.getSrc();
        // get unfinished append file diff
        List<FileDiff> unfinishedAppendFileDiff = new ArrayList<>();
        FileDiff renameDiffInArchive = null;
        for (FileDiff fileDiff : fileDiffArchive) {
          if (fileDiff.getDiffId() == renameFileDiff.getDiffId()) {
            renameDiffInArchive = fileDiff;
            break;
          }
          String pathWithSlash = path.endsWith("/") ? path : path + "/";
          String srcWithSlash = fileDiff.getSrc().endsWith("/") ?
              fileDiff.getSrc() : fileDiff.getSrc() + "/";
          if (fileDiff.getDiffType() != FileDiffType.APPEND ||
              !srcWithSlash.startsWith(pathWithSlash)) {
            continue;
          }
          if (fileDiff.getState() == FileDiffState.PENDING) {
            unfinishedAppendFileDiff.add(fileDiff);
          }
        }
        if (unfinishedAppendFileDiff.isEmpty()) {
          return true;
        } else {
          for (FileDiff unfinished : unfinishedAppendFileDiff) {
            FileDiff fileDiff = fileDiffCache.get(unfinished.getDiffId());
            if (fileDiff == null) {
              fileDiff = unfinished;
            }
            fileDiffTerminatedInternal(fileDiff);
            updateFileDiffInCache(fileDiff.getDiffId(), FileDiffState.FAILED);
            // add a new append file diff with new name
            FileDiff newFileDiff = new FileDiff(FileDiffType.APPEND, FileDiffState.PENDING);
            newFileDiff.getParameters().putAll(fileDiff.getParameters());
            newFileDiff.setSrc(fileDiff.getSrc().replaceFirst(
                renameFileDiff.getSrc(), renameFileDiff.getParameters().get("-dest")));
            long did = metaStore.insertFileDiff(newFileDiff);
            newFileDiff.setDiffId(did);
            fileDiffArchive.add(fileDiffArchive.indexOf(renameDiffInArchive), newFileDiff);
          }
          return false;
        }
      }

      long getHead() {
        if (diffChain.size() == 0) {
          return -1;
        }
        return diffChain.get(0);
      }

      long removeHead() {
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

      void removeFromChain(FileDiff fileDiff) {
        Iterator<Long> iter = diffChain.iterator();
        while (iter.hasNext()) {
          if (iter.next() == fileDiff.getDiffId()) {
            iter.remove();
          }
        }
        if (diffChain.size() == 0) {
          fileDiffChainMap.remove(filePath);
        }
      }

      void markAllDiffs() throws MetaStoreException {
        List<Long> dids = new ArrayList<>();
        for (long did : diffChain) {
          if (fileDiffCache.containsKey(did)) {
            updateFileDiffInCache(did, FileDiffState.MERGED);
          } else {
            dids.add(did);
            LOG.error("FileDiff {} is in chain but not in cache", did);
          }
        }
        metaStore.batchUpdateFileDiff(dids, FileDiffState.MERGED);
        diffChain.clear();
        currAppendLength = 0;
        appendChain.clear();
      }
    }
  }

  private class PurgeFileDiffTask implements Runnable {
    public int maxNumRecords;

    public PurgeFileDiffTask(Configuration conf){
      this.maxNumRecords = conf.getInt(SmartConfKeys.SMART_FILE_DIFF_MAX_NUM_RECORDS_KEY,
          SmartConfKeys.SMART_FILE_DIFF_MAX_NUM_RECORDS_DEFAULT);
    }

    @Override
    public void run() {
      if (numFileDiffUseless.get() <= maxNumRecords) {
        return;
      }
      try {
        numFileDiffUseless.addAndGet(-metaStore.deleteUselessFileDiff(maxNumRecords));
      } catch (MetaStoreException e) {
        LOG.error("Error occurs when delete useless file diff!");
      }
    }
  }
}
