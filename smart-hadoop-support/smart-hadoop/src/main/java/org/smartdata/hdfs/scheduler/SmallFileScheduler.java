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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.hdfs.action.SmallFileUncompactAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SmallFileScheduler extends ActionSchedulerService {
  private final URI nnUri;
  private DFSClient dfsClient;
  private MetaStore metaStore;

  /**
   * Container file lock, and whether exist in hdfs already.
   */
  private Map<String, Boolean> containerFileLock;

  /**
   * Compact small file lock.
   */
  private List<String> compactSmallFileLock;

  /**
   * Uncompact small file lock.
   */
  private List<String> uncompactSmallFileLock;

  /**
   * Cache all the container files of SSM.
   */
  private List<String> containerFileCache;

  /**
   * Compact file state queue for caching these file state to update.
   */
  private Queue<CompactFileStateDiff> compactFileStateQueue;

  /**
   * Scheduled service to update meta store.
   */
  private ScheduledExecutorService executorService;

  private static final String COMPACT_ACTION_NAME = "compact";
  private static final String UNCOMPACT_ACTION_NAME = "uncompact";
  private static final List<String> ACTIONS = Arrays.asList("compact", "uncompact");
  public static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore)
      throws IOException {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
  }

  @Override
  public void init() throws IOException {
    this.containerFileLock = new ConcurrentHashMap<>();
    this.compactFileStateQueue = new ConcurrentLinkedQueue<>();
    this.containerFileCache = Collections.synchronizedList(new ArrayList<String>());
    this.compactSmallFileLock = Collections.synchronizedList(new ArrayList<String>());
    this.uncompactSmallFileLock = Collections.synchronizedList(new ArrayList<String>());
    this.dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() throws IOException {
    executorService.scheduleAtFixedRate(
        new ScheduleTask(), 5000, 1000,
        TimeUnit.MILLISECONDS);
    try {
      List<String> containerFileList = metaStore.getAllContainerFiles();
      this.containerFileCache.addAll(containerFileList);
    } catch (MetaStoreException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> getSupportedActions() {
    return ACTIONS;
  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) {
    if (COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Check if container file is null
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);
      if (containerFilePath == null || containerFilePath.isEmpty()) {
        LOG.debug("Illegal container file path: " + containerFilePath);
        return false;
      }

      // Check if small files is null or empty
      String smallFiles = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      if (smallFiles == null || smallFiles.isEmpty()) {
        LOG.debug("Illegal small files: " + smallFiles);
        return false;
      }

      // Check if small file list is empty
      // Check if small file is container file or locked
      ArrayList<String> smallFileList = new Gson().fromJson(
          smallFiles, new TypeToken<ArrayList<String>>() {
          }.getType());
      if (smallFileList.isEmpty()) {
        LOG.debug("Illegal small file list: " + smallFileList);
        return false;
      } else {
        for (String smallFile : smallFileList) {
          if (containerFileCache.contains(smallFile)) {
            LOG.debug(smallFile + " is container file.");
            return false;
          }
          if (compactSmallFileLock.contains(smallFile)) {
            LOG.debug(smallFile + " is locked.");
            return false;
          }
        }
      }

      return true;
    } else if (UNCOMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Check if container file is not null and exist
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileUncompactAction.CONTAINER_FILE);
      try {
        if (containerFilePath == null
            || containerFilePath.isEmpty()
            || !dfsClient.exists(containerFilePath)) {
          LOG.debug("Illegal container file path: " + containerFilePath);
          return false;
        }
      } catch (IOException e) {
        LOG.error("Failed to check if container file exists: " + containerFilePath);
        return false;
      }

      return true;
    } else {
      return true;
    }
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Get container file and small file list of this action
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);
      String smallFiles = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      ArrayList<String> smallFileList = new Gson().fromJson(
          smallFiles, new TypeToken<ArrayList<String>>() {
          }.getType());

      // Check if container file is locked and retry
      if (containerFileLock.containsKey(containerFilePath)) {
        return ScheduleResult.RETRY;
      } else {
        // Check if small file path is valid and unlocked
        for (String smallFile : smallFileList) {
          if (smallFile == null || smallFile.isEmpty()) {
            LOG.debug("Illegal small file path: " + smallFile);
            actionInfo.setResult("Illegal small file path: " + smallFile);
            return ScheduleResult.FAIL;
          } else if (compactSmallFileLock.contains(smallFile)) {
            String errMsg = String.format("%s is locked.", smallFile);
            LOG.debug(errMsg);
            actionInfo.setResult(errMsg);
            return ScheduleResult.FAIL;
          } else if (containerFileCache.contains(smallFile)) {
            String errMsg = String.format("%s is not small file.", smallFile);
            LOG.debug(errMsg);
            actionInfo.setResult(errMsg);
            return ScheduleResult.FAIL;
          }
        }

        // Get small file state map from meta store.
        Map<String, FileState> fileStateMap;
        try {
          fileStateMap = metaStore.getFileStates(smallFileList);
        } catch (MetaStoreException e) {
          LOG.error("Failed to get file states of small files. " + e.toString());
          actionInfo.setResult(
              "Failed to get file states of small files. " + e.toString());
          return ScheduleResult.FAIL;
        }

        // Check if the state of small file is NORMAL
        if (fileStateMap.size() != 0) {
          for (String smallFile : smallFileList) {
            if (fileStateMap.containsKey(smallFile)) {
              FileState.FileType smallFileType = fileStateMap.get(
                  smallFile).getFileType();
              if (smallFileType != FileState.FileType.NORMAL) {
                String errMsg = String.format(
                    "%s has invalid file state %s for small file compact.",
                    smallFile, smallFileType.toString());
                LOG.debug(errMsg);
                actionInfo.setResult(errMsg);
                return ScheduleResult.FAIL;
              }
            }
          }
        }

        try {
          // Lock container file and set whether exists already
          // Lock small files
          boolean exist = dfsClient.exists(containerFilePath);
          containerFileLock.put(containerFilePath, exist);
          compactSmallFileLock.addAll(smallFileList);
        } catch (IOException e) {
          String errMsg = String.format(
              "Failed to check if the container file is exists: %s for %s.",
              containerFilePath, e.toString());
          LOG.error(errMsg);
          actionInfo.setResult(errMsg);
          return ScheduleResult.FAIL;
        }
      }

      return ScheduleResult.SUCCESS;
    } else if (UNCOMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Get container file and small file list of this action
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);

      // Check if container file is locked
      if (!containerFileLock.containsKey(containerFilePath)) {
        // Get small files of the container file
        List<String> smallFileList;
        try {
          smallFileList = metaStore.getSmallFilesByContainerFile(containerFilePath);
        } catch (MetaStoreException e) {
          String errMsg = String.format(
              "Failed to get small files of the container file: %s for %s.",
              containerFilePath, e.toString());
          LOG.error(errMsg);
          actionInfo.setResult(errMsg);
          return ScheduleResult.FAIL;
        }

        if (smallFileList != null && !smallFileList.isEmpty()) {
          // Check if small file path is valid and unlocked
          for (String smallFile : smallFileList) {
            if (smallFile == null || smallFile.isEmpty()) {
              LOG.debug("Illegal small file path: " + smallFile);
              actionInfo.setResult("Illegal small file path: " + smallFile);
              return ScheduleResult.FAIL;
            } else if (uncompactSmallFileLock.contains(smallFile)) {
              LOG.debug("The small file is locked: " + smallFile);
              actionInfo.setResult("The small file is locked: " + smallFile);
              return ScheduleResult.FAIL;
            } else if (containerFileCache.contains(smallFile)) {
              LOG.debug("This file is not small file: " + smallFile);
              actionInfo.setResult("This file is not small file: " + smallFile);
              return ScheduleResult.FAIL;
            }
          }

          // Update container file and uncompact small file lock
          containerFileLock.put(containerFilePath, true);
          uncompactSmallFileLock.addAll(smallFileList);

          // Put small files into arguments of this action
          Map<String, String> args = new HashMap<>(2);
          args.put(HdfsAction.FILE_PATH, new Gson().toJson(smallFileList));
          args.put(SmallFileUncompactAction.CONTAINER_FILE,
              actionInfo.getArgs().get(SmallFileUncompactAction.CONTAINER_FILE));
          action.setArgs(args);
          actionInfo.setArgs(args);
          return ScheduleResult.SUCCESS;
        } else {
          return ScheduleResult.FAIL;
        }
      } else {
        // Retry if container file is locked
        return ScheduleResult.RETRY;
      }
    } else {
      return ScheduleResult.SUCCESS;
    }
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.isFinished()) {
      if (COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
        // Get container file path, small files, result of this action
        String containerFilePath = actionInfo.getArgs().get(
            SmallFileCompactAction.CONTAINER_FILE);
        List<String> smallFileList = new Gson().fromJson(
            actionInfo.getArgs().get(HdfsAction.FILE_PATH),
            new TypeToken<ArrayList<String>>() {
            }.getType());
        List<CompactFileState> compactFileStates = new Gson().fromJson(
            actionInfo.getResult(),
            new TypeToken<ArrayList<CompactFileState>>() {
            }.getType());

        // Update compact file state queue
        for (CompactFileState compactFileState : compactFileStates) {
          compactFileStateQueue.add(new CompactFileStateDiff(
              true, compactFileState));
        }

        if (compactFileStates.size() == 0 || !actionInfo.isSuccessful()) {
          try {
            if (!containerFileLock.get(containerFilePath)) {
              if (dfsClient.getFileInfo(containerFilePath).getLen() == 0) {
                // Delete container file if not exists before running action,
                // and no small file compacted to it
                containerFileCache.remove(containerFilePath);
                dfsClient.delete(containerFilePath, false);
              } else {
                containerFileCache.add(containerFilePath);
              }
            }
          } catch (IOException e) {
            LOG.error("Failed to handle container file: "
                + containerFilePath, e);
          }
        } else {
          containerFileCache.add(containerFilePath);
        }

        // Remove locks of container file and small files
        containerFileLock.remove(containerFilePath);
        compactSmallFileLock.removeAll(smallFileList);
      } else if (UNCOMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
        // Get container file path, small files, result of this action
        String containerFilePath = actionInfo.getArgs().get(
            SmallFileUncompactAction.CONTAINER_FILE);
        List<String> smallFileList = new Gson().fromJson(
            actionInfo.getArgs().get(HdfsAction.FILE_PATH),
            new TypeToken<ArrayList<String>>() {
            }.getType());
        List<String> removeSmallFiles = new Gson().fromJson(
            actionInfo.getResult(),
            new TypeToken<ArrayList<String>>() {
            }.getType());

        // Update compact file state map
        for (String removeSmallFile : removeSmallFiles) {
          compactFileStateQueue.add(new CompactFileStateDiff(
              false, new CompactFileState(removeSmallFile, null)));
        }

        if (removeSmallFiles.size() > 0) {
          if (actionInfo.isSuccessful()) {
            try {
              // Delete container file if action is successful
              containerFileCache.remove(containerFilePath);
              dfsClient.delete(containerFilePath, false);
            } catch (IOException e) {
              LOG.error("Failed to delete container file: "
                  + containerFilePath, e);
            }
          } else {
            try {
              List<String> list = metaStore.getSmallFilesByContainerFile(containerFilePath);
              if (list == null || list.isEmpty()) {
                // Delete container file if the container file does not have any small file
                containerFileCache.remove(containerFilePath);
                dfsClient.delete(containerFilePath, false);
              }
            } catch (MetaStoreException e1) {
              LOG.error("Failed to get small files of the container file: "
                  + containerFilePath, e1);
            } catch (IOException e2) {
              LOG.error("Failed to delete container file: "
                  + containerFilePath, e2);
            }
          }
        }

        // Remove locks of container file and small files
        containerFileLock.remove(containerFilePath);
        uncompactSmallFileLock.removeAll(smallFileList);
      }
    }
  }

  /**
   * An inner class for updating compact file state conveniently.
   */
  private class CompactFileStateDiff {
    boolean isInsert;
    CompactFileState compactFileState;

    private CompactFileStateDiff(boolean isInsert,
        CompactFileState compactFileState) {
      this.isInsert = isInsert;
      this.compactFileState = compactFileState;
    }
  }

  /**
   * Sync compact file states with meta store.
   */
  private void syncMetaStore() {
    List<CompactFileState> compactFileStates = new ArrayList<>();
    List<String> unCompactFiles = new ArrayList<>();
    while (true) {
      CompactFileStateDiff diff = compactFileStateQueue.poll();
      if (diff != null) {
        try {
          if (diff.isInsert) {
            FileInfo info = metaStore.getFile(diff.compactFileState.getPath());
            if (info != null && info.getLength() == 0) {
              compactFileStates.add(diff.compactFileState);
            } else {
              compactFileStateQueue.offer(diff);
            }
          } else {
            unCompactFiles.add(diff.compactFileState.getPath());
          }
        } catch (MetaStoreException e) {
          LOG.error("Failed to get file info. " + e.toString());
          compactFileStateQueue.offer(diff);
        }
      } else {
        try {
          if (compactFileStates.size() > 0) {
            metaStore.insertCompactFileStates(
                compactFileStates.toArray(new CompactFileState[0]));
          }
          if (unCompactFiles.size() > 0) {
            metaStore.deleteCompactFileStates(unCompactFiles);
          }
          return;
        } catch (MetaStoreException e) {
          LOG.error("Failed to update file state of meta store. " + e.toString());
          return;
        }
      }
    }
  }

  /**
   * Scheduled task to sync meta store.
   */
  private class ScheduleTask implements Runnable {
    @Override
    public void run() {
      try {
        syncMetaStore();
      } catch (Throwable t) {
        LOG.error("Failed to sync compact file states with meta store. " + t.toString());
      }
    }
  }

  @Override
  public void stop() throws IOException {
    try {
      syncMetaStore();
    } catch (Exception e) {
      throw new IOException(e);
    }
    executorService.shutdown();
  }
}
