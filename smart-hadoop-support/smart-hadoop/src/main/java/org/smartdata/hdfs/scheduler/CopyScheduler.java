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
import org.smartdata.metastore.ActionSchedulerService;
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
  private Map<String, ScheduleTask.FileChain> fileDiffChainMap;
  // <did, Fail times>
  private Map<Long, Integer> fileDiffMap;
  // Merge append length threshold
  private long mergeLenTh = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT * 3;
  // Merge count length threshold
  private long mergeCountTh = 10;
  private int retryTh = 3;

  public CopyScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.fileLock = new ConcurrentHashMap<>();
    this.actionDiffMap = new ConcurrentHashMap<>();
    this.fileDiffChainMap = new HashMap<>();
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
    if (fileLock.containsKey(path)) {
      // File is currently under sync
      return ScheduleResult.FAIL;
    }
    if (!fileDiffChainMap.containsKey(path)) {
      // File Chain is not ready
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
    try {
      fileDiff = metaStore.getFileDiff(fid);
    } catch (MetaStoreException e) {
      LOG.error("Get file diff by did = {} from metastore error", fid, e);
    }
    if (fileDiff == null) {
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
  }

  public void onPreDispatch(LaunchAction action) {
  }

  public void onActionFinished(ActionInfo actionInfo) {
    // Remove lock
    FileDiff fileDiff = null;
    if(actionInfo.isFinished()) {
      try {
        long did = actionDiffMap.get(actionInfo.getActionId());
        // Remove for action diff map
        if (actionDiffMap.containsKey(actionInfo.getActionId())) {
          actionDiffMap.remove(actionInfo.getActionId());
        }
        fileDiff = metaStore.getFileDiff(did);
        if (fileDiff == null) {
          return;
        }
        if (actionInfo.isSuccessful()) {
          if (fileDiffChainMap.containsKey(fileDiff.getSrc())) {
            // Remove from chain top
            fileDiffChainMap.get(fileDiff.getSrc()).removeHead();
          }
          metaStore.updateFileDiff(did, FileDiffState.APPLIED);
          if (fileDiffMap.containsKey(did)) {
            fileDiffMap.remove(did);
          }
        } else {
          if (fileDiffMap.containsKey(did)) {
            int curr = fileDiffMap.get(did);
            if (curr >= retryTh) {
              metaStore.updateFileDiff(did, FileDiffState.FAILED);
              directSync(fileDiff.getSrc(), actionInfo.getArgs().get(SyncAction.SRC),
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

  private void baseSync(String srcDir, String destDir) throws MetaStoreException {
    List<FileInfo> srcFiles = metaStore.getFilesByPrefix(srcDir);
    for (FileInfo fileInfo : srcFiles) {
      if (fileInfo.isdir()) {
        // Ignore directory
        continue;
      }
      String src = fileInfo.getPath();
      // TODO maybe too long for large directory
      directSync(src, srcDir, destDir);
    }
  }


  private void directSync(String src, String srcDir, String destDir) throws MetaStoreException {
    String dest = src.replace(srcDir, destDir);
    FileInfo fileInfo = metaStore.getFile(src);
    if (fileInfo == null) {
      // Primary file doesn't exist
      return;
    }
    // Mark all related diff as Merged
    if (fileDiffChainMap.containsKey(src)) {
      fileDiffChainMap.get(src).markAllDiffs();
      fileDiffChainMap.remove(src);
      // Unlock file
      if (fileLock.containsKey(src)) {
        fileLock.remove(src);
      }
    }
    List<FileDiff> fileDiffs = metaStore.getFileDiffsByFileName(src);
    for (FileDiff fileDiff :fileDiffs) {
      if (fileDiff.getState() == FileDiffState.PENDING) {
        metaStore.updateFileDiff(fileDiff.getDiffId(), FileDiffState.MERGED);
      }
    }
    FileDiff fileDiff;
    long offSet = fileCompare(fileInfo, dest);
    if (offSet == fileInfo.getLength()) {
      LOG.debug("Primary len={}, remote len={}", fileInfo.getLength(), offSet);
      return;
    } else if (offSet > fileInfo.getLength()) {
      // Remove dirty remote file
      fileDiff = new FileDiff(FileDiffType.DELETE, FileDiffState.PENDING);
      fileDiff.setSrc(src);
      metaStore.insertFileDiff(fileDiff);
    }
    // Copy tails to remote
    fileDiff = new FileDiff(FileDiffType.APPEND, FileDiffState.PENDING);
    fileDiff.setSrc(src);
    // Append changes to remote files
    fileDiff.getParameters().put("-length", String.valueOf(fileInfo.getLength() - offSet));
    fileDiff.getParameters().put("-offset", String.valueOf(offSet));
    fileDiff.setRuleId(-1);
    metaStore.insertFileDiff(fileDiff);
  }

  private long fileCompare(FileInfo fileInfo, String dest) throws MetaStoreException {
    // Primary
    long localLen = fileInfo.getLength();
    Configuration conf = null;
    try {
      conf = getContext().getConf();
    } catch (NullPointerException e) {
      conf = new Configuration();
    }
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
        pendingDiffs = metaStore.getPendingDiff();
        diffMerge(pendingDiffs);
      } catch (MetaStoreException e) {
        LOG.debug("Sync fileDiffs error", e);
      }
    }

    // private void addToRunning() {
    //   if (fileDiffMap.size() == 0) {
    //     return;
    //   }
    //   for (FileChain fileChain: fileDiffChainMap.values()) {
    //     try {
    //       fileChain.addTopRunning();
    //     } catch (MetaStoreException e) {
    //       LOG.debug("Add fileDiffs to Pending list error", e);
    //       continue;
    //     }
    //   }
    // }

    private void diffMerge(List<FileDiff> fileDiffs) throws MetaStoreException {
      // Merge all existing fileDiffs into fileChains
      LOG.debug("Size of Pending diffs", fileDiffs.size());
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
          mergeRename(fileDiff);
        } else if (fileDiff.getDiffType() == FileDiffType.DELETE) {
          mergeDelete();
          if (nameChain.size() > 1) {
            fileDiff.setSrc(nameChain.get(0));
            // Delete raw is enough
            metaStore.updateFileDiff(did, nameChain.get(0));
          }
        }
        diffChain.add(did);
      }

      @VisibleForTesting
      void mergeAppend() throws MetaStoreException {
        if (fileLock.containsKey(filePath)) {
          return;
        }
        // Lock file to avoid File Chain being processed
        fileLock.put(filePath, -1L);
        long offset = Integer.MAX_VALUE;
        long totalLength = 0;
        long lastAppend = -1;
        for (long did : appendChain) {
          FileDiff fileDiff = metaStore.getFileDiff(did);
          if (fileDiff != null && fileDiff.getState().getValue() != 2) {
            long currOffset = Long.valueOf(fileDiff.getParameters().get("-offset"));
            if (offset > currOffset) {
              offset = currOffset;
            }
            if (currOffset != offset && currOffset != totalLength + offset) {
              // offset and length check to avoid dirty append
              break;
            }
            metaStore.updateFileDiff(did, FileDiffState.APPLIED);
            // Add current file length to length
            totalLength += Long.valueOf(fileDiff.getParameters().get("-length"));
            lastAppend = did;
          }
        }
        if (lastAppend == -1) {
          return;
        }
        FileDiff fileDiff = metaStore.getFileDiff(lastAppend);
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
      void mergeDelete() throws MetaStoreException {
        // TODO if create diff is in append
        for (long did : diffChain) {
          metaStore.updateFileDiff(did, FileDiffState.APPLIED);
        }
        diffChain.clear();
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
        // Lock file to avoid File Chain being processed
        fileLock.put(filePath, -1L);
        String newName = fileDiff.getParameters().get("-dest");
        nameChain.add(newName);
        for (long did : appendChain) {
          FileDiff appendFileDiff = metaStore.getFileDiff(did);
          if (appendFileDiff != null && appendFileDiff.getState().getValue() != 2) {
            appendFileDiff.setSrc(newName);
            metaStore.updateFileDiff(did, newName);
          }
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

      // public void addTopRunning() throws MetaStoreException {
      //   if (fileLock.containsKey(filePath)) {
      //     return;
      //   }
      //   if (diffChain.size() == 0) {
      //     return;
      //   }
      //   long fid = diffChain.get(0);
      //   metaStore.updateFileDiff(fid, FileDiffState.RUNNING);
      //   // fileLock.put(filePath, fid);
      // }
    }
  }
}
