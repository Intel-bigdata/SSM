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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
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
import java.util.Iterator;
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
  private Map<String, Long> fileLock;
  private Map<Long, Long> actionDiffMap;
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
    String srcDir = action.getArgs().get("-src");
    String path = action.getArgs().get("-file");
    String destDir = action.getArgs().get("-dest");
    if (!fileChainMap.containsKey(path)) {
      return ScheduleResult.FAIL;
    }
    long fid = fileChainMap.get(path).popTop();
    if (fid == -1) {
      // FileChain is already empty
      return ScheduleResult.FAIL;
    } 
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
        action.getArgs().put("-dest", fileDiff.getParameters().get("-dest").replace(srcDir, destDir));
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

  public void forceSync(String src, String dest) throws IOException, MetaStoreException {
    List<FileInfo> srcFiles = metaStore.getFilesByPrefix(src);
    for (FileInfo fileInfo : srcFiles) {
      if (fileInfo.isdir()) {
        // Ignore directory
        continue;
      }
      String fullPath = fileInfo.getPath();
      String remotePath = fullPath.replace(src, dest);
      long offSet = fileCompare(fileInfo, remotePath);
      if (offSet >= fileInfo.getLength()) {
        LOG.debug("Primary len={}, remote len={}", fileInfo.getLength(), offSet);
        continue;
      }
      FileDiff fileDiff = new FileDiff(FileDiffType.APPEND, FileDiffState.RUNNING);
      fileDiff.setSrc(fullPath);
      // Append changes to remote files
      fileDiff.getParameters().put("-length", String.valueOf(fileInfo.getLength() - offSet));
      fileDiff.getParameters().put("-offset", String.valueOf(offSet));
      fileDiff.setRuleId(-1);
      metaStore.insertFileDiff(fileDiff);
    }
  }

  private long fileCompare(FileInfo fileInfo, String dest) throws MetaStoreException {
    // Primary
    long localLen = fileInfo.getLength();
    // TODO configuration
    Configuration conf = new Configuration();
    // Get InputStream from URL
    FileSystem fs = null;
    try {
      fs = FileSystem.get(URI.create(dest), conf);
      long remoteLen = fs.getFileStatus(new Path(dest)).getLen();
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


  private class ScheduleTask implements Runnable {

    private Map<Long, FileDiff> fileDiffBatch;

    private void syncActions() {
      for (Iterator<Map.Entry<Long, Long>> it = actionDiffMap.entrySet().iterator(); it.hasNext();) {
        Map.Entry<Long, Long> entry = it.next();
        try {
          ActionInfo actionInfo = metaStore.getActionById(entry.getKey());
          if(actionInfo.isSuccessful()) {
            metaStore.markFileDiffApplied(entry.getValue(), FileDiffState.APPLIED);
            FileDiff fileDiff = metaStore.getFileDiff(entry.getValue());
            // Remove lock
            fileLock.remove(fileDiff.getSrc());
            // Add to pending list
            fileChainMap.get(fileDiff.getSrc()).addTopRunning();
            it.remove();
          }
        } catch (MetaStoreException e) {
          LOG.error("Sync diff actions status fails, key " + entry.getKey() +
              " value " + entry.getValue(), e);
        }
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
        fileDiffBatch.put(fileDiff.getDiffId(), fileDiff);
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
          fileChain.clear();
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
    
    @Override
    public void run() {
      syncActions();
      // Sync backup rules
      // Sync/schedule file diffs
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
        fileLock.put(filePath, fid);
      }
    }
  }
}
