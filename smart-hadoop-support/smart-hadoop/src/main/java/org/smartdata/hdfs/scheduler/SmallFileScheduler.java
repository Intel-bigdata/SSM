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
import org.smartdata.SmartFilePermission;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.hdfs.action.SmallFileUncompactAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.WhitelistHelper;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.LaunchCmdlet;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.smartdata.model.ActionInfo.OLD_FILE_ID;

public class SmallFileScheduler extends ActionSchedulerService {
  private MetaStore metaStore;

  /**
   * Container file lock.
   */
  private Set<String> containerFileLock;

  /**
   * Compact small file lock.
   */
  private Set<String> compactSmallFileLock;

  /**
   * Cache all the container files of SSM.
   */
  private Set<String> containerFileCache;

  /**
   * Small files which waiting to be handled.
   */
  private Set<String> handlingSmallFileCache;

  /**
   * Compact file state queue for caching these file state to update.
   */
  private Queue<CompactFileState> compactFileStateQueue;

  /**
   * Scheduled service to update meta store.
   */
  private ScheduledExecutorService executorService;

  private static final int META_STORE_INSERT_BATCH_SIZE = 200;
  public static final String COMPACT_ACTION_NAME = "compact";
  public static final String UNCOMPACT_ACTION_NAME = "uncompact";
  public static final List<String> ACTIONS =
      Arrays.asList(COMPACT_ACTION_NAME, UNCOMPACT_ACTION_NAME);
  private DFSClient dfsClient;
  private static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.metaStore = metaStore;
  }

  @Override
  public void init() throws IOException {
    this.containerFileLock = Collections.synchronizedSet(new HashSet<String>());
    this.compactSmallFileLock = Collections.synchronizedSet(new HashSet<String>());
    this.containerFileCache = Collections.synchronizedSet(new HashSet<String>());
    this.handlingSmallFileCache = Collections.synchronizedSet(new HashSet<String>());
    this.compactFileStateQueue = new ConcurrentLinkedQueue<>();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    try {
      final URI nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
      dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
    } catch (IOException e) {
      LOG.warn("Failed to create dfsClient.");
    }
  }

  @Override
  public void start() throws IOException {
    executorService.scheduleAtFixedRate(
        new ScheduleTask(), 100, 50,
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
  public boolean onSubmit(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex)
      throws IOException {
    // check args
    if (actionInfo.getArgs() == null) {
      throw new IOException("No arguments for the action");
    }
    if (COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Check if container file is null
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);
      if (containerFilePath == null || containerFilePath.isEmpty()) {
        throw new IOException("Illegal container file path: " + containerFilePath);
      }

      // Check if small files is null or empty
      String smallFiles = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      if (smallFiles == null || smallFiles.isEmpty()) {
        throw new IOException("Illegal small files: " + smallFiles);
      }

      // Check if small file list converted from Json is not empty
      ArrayList<String> smallFileList = new Gson().fromJson(
          smallFiles, new TypeToken<ArrayList<String>>() {
          }.getType());
      if (smallFileList.isEmpty()) {
        throw new IOException("Illegal small files list: " + smallFileList);
      }

      // Check whitelist
      if (WhitelistHelper.isEnabled(getContext().getConf())) {
        for (String filePath : smallFileList) {
          if (!WhitelistHelper.isInWhitelist(filePath, getContext().getConf())) {
            throw new IOException("Path " + filePath + " is not in the whitelist.");
          }
        }
      }

      // Check if the small file list is valid
      if (checkIfValidSmallFiles(smallFileList)) {
        return true;
      } else {
        throw new IOException("Illegal small files are provided.");
      }
    } else {
      return true;
    }
  }

  /**
   * Check if the small file list is valid.
   */
  private boolean checkIfValidSmallFiles(List<String> smallFileList) {
    for (String smallFile : smallFileList) {
      if (smallFile == null || smallFile.isEmpty()) {
        LOG.error("Illegal small file path: {}", smallFile);
        return false;
      } else if (compactSmallFileLock.contains(smallFile)) {
        LOG.error(String.format("%s is locked.", smallFile));
        return false;
      } else if (handlingSmallFileCache.contains(smallFile)) {
        LOG.error(String.format("%s is being handling.", smallFile));
        return false;
      } else if (containerFileCache.contains(smallFile)
          || containerFileLock.contains(smallFile)) {
        LOG.error(String.format("%s is container file.", smallFile));
        return false;
      }
    }

    // Get small file info list and file state map from meta store.
    List<FileInfo> fileInfos;
    Map<String, FileState> fileStateMap;
    try {
      fileInfos = metaStore.getFilesByPaths(smallFileList);
      fileStateMap = metaStore.getFileStates(smallFileList);
    } catch (MetaStoreException e) {
      LOG.error("Failed to get file states of small files.", e);
      return false;
    }

    // Get small file info map
    Map<String, FileInfo> fileInfoMap = new HashMap<>();
    for (FileInfo fileInfo : fileInfos) {
      fileInfoMap.put(fileInfo.getPath(), fileInfo);
    }

    // Check if the permission of small file is same,
    // and all the small files exist
    FileInfo firstFileInfo = null;
    for (String smallFile : smallFileList) {
      FileInfo fileInfo = fileInfoMap.get(smallFile);
      if (fileInfo != null) {
        if (firstFileInfo == null) {
          firstFileInfo = fileInfo;
        } else {
          if (!(new SmartFilePermission(firstFileInfo)).equals(
              new SmartFilePermission(fileInfo))) {
            LOG.debug(String.format(
                "%s has different file permission with %s.",
                firstFileInfo.getPath(), fileInfo.getPath()));
            return false;
          }
        }
      } else {
        LOG.debug("{} is not exist!!!", smallFile);
        return false;
      }
    }

    // Check if the state of small file is NORMAL
    for (Map.Entry<String, FileState> entry : fileStateMap.entrySet()) {
      String smallFile = entry.getKey();
      FileState.FileType smallFileType = entry.getValue().getFileType();
      if (smallFileType != FileState.FileType.NORMAL) {
        LOG.debug(String.format(
            "%s has invalid file state %s for small file compact.",
            smallFile, smallFileType.toString()));
        return false;
      }
    }

    return true;
  }

  /**
   * Get container file info according to action arguments and meta store.
   */
  private SmartFilePermission getContainerFilePermission(ActionInfo actionInfo,
      String containerFilePath) throws MetaStoreException, IllegalArgumentException {
    // Get container file permission from the argument of this action
    String containerFilePermissionArg = actionInfo.getArgs().get(
        SmallFileCompactAction.CONTAINER_FILE_PERMISSION);
    SmartFilePermission containerFilePermissionFromArg = null;
    if (containerFilePermissionArg != null && !containerFilePermissionArg.isEmpty()) {
      containerFilePermissionFromArg = new Gson().fromJson(
          containerFilePermissionArg, new TypeToken<SmartFilePermission>() {
          }.getType());
    }

    // Get container file permission from meta store
    SmartFilePermission containerFilePermissionFromMeta = null;
    FileInfo containerFileInfo = metaStore.getFile(containerFilePath);
    if (containerFileInfo != null) {
      containerFilePermissionFromMeta = new SmartFilePermission(containerFileInfo);
    }

    // Get container file permission
    SmartFilePermission containerFilePermission;
    if (containerFilePermissionFromArg == null
        || containerFilePermissionFromMeta == null) {
      containerFilePermission = (containerFilePermissionFromArg == null) ?
          containerFilePermissionFromMeta : containerFilePermissionFromArg;
    } else {
      if (containerFilePermissionFromArg.equals(containerFilePermissionFromMeta)) {
        containerFilePermission = containerFilePermissionFromArg;
      } else {
        throw new IllegalArgumentException(
            "Illegal container file permission argument.");
      }
    }
    return containerFilePermission;
  }

  /**
   * Get compact action schedule result according to action info.
   */
  private ScheduleResult getCompactScheduleResult(ActionInfo actionInfo) {
    // Get container file and small file list of this action
    String containerFilePath = actionInfo.getArgs().get(
        SmallFileCompactAction.CONTAINER_FILE);
    ArrayList<String> smallFileList = new Gson().fromJson(
        actionInfo.getArgs().get(HdfsAction.FILE_PATH),
        new TypeToken<ArrayList<String>>() {
        }.getType());

    // Check if container file is locked and retry
    if (containerFileLock.contains(containerFilePath)) {
      return ScheduleResult.RETRY;
    } else {
      // Check if the small file list is valid
      if (!checkIfValidSmallFiles(smallFileList)) {
        actionInfo.setResult("Small file list is invalid.");
        return ScheduleResult.FAIL;
      }

      // Get container file permission
      SmartFilePermission containerFilePermission;
      try {
        containerFilePermission = getContainerFilePermission(
            actionInfo, containerFilePath);
      } catch (MetaStoreException e1) {
        actionInfo.setResult(String.format(
            "Failed to get file info of the container file %s for %s.",
            containerFilePath, e1.toString()));
        return ScheduleResult.FAIL;
      } catch (IllegalArgumentException e2) {
        actionInfo.setResult(e2.getMessage());
        return ScheduleResult.FAIL;
      }

      // Get first small file info
      FileInfo firstFileInfo;
      SmartFilePermission firstFilePermission;
      try {
        firstFileInfo = metaStore.getFile(smallFileList.get(0));
        firstFilePermission = new SmartFilePermission(firstFileInfo);
      } catch (MetaStoreException e) {
        actionInfo.setResult(String.format(
            "Failed to get first file info: %s.", containerFilePath));
        return ScheduleResult.FAIL;
      }

      // Reset action arguments if container file is not exist
      // and its permission is null
      if (containerFilePermission == null) {
        Map<String, String> args = new HashMap<>(3);
        args.put(SmallFileCompactAction.CONTAINER_FILE,
            actionInfo.getArgs().get(SmallFileCompactAction.CONTAINER_FILE));
        args.put(SmallFileCompactAction.FILE_PATH,
            new Gson().toJson(smallFileList));
        args.put(SmallFileCompactAction.CONTAINER_FILE_PERMISSION,
            new Gson().toJson(firstFilePermission));
        actionInfo.setArgs(args);
      } else {
        if (!containerFilePermission.equals(firstFilePermission)) {
          actionInfo.setResult(String.format(
              "Container file %s has different permission with %s.",
              containerFilePath, firstFileInfo.getPath()));
          return ScheduleResult.FAIL;
        }
      }

      // Lock container file and small files
      containerFileLock.add(containerFilePath);
      compactSmallFileLock.addAll(smallFileList);
      afterSchedule(actionInfo);
      return ScheduleResult.SUCCESS;
    }
  }

  /**
   * Get uncompact action schedule result according to action info,
   * and reset action arguments.
   */
  private ScheduleResult getUncompactScheduleResult(ActionInfo actionInfo,
      LaunchAction action) {
    // Check if container file path is valid
    String containerFilePath = actionInfo.getArgs().get(
        SmallFileCompactAction.CONTAINER_FILE);
    if (containerFilePath == null || containerFilePath.isEmpty()) {
      LOG.debug("Illegal container file path: {}", containerFilePath);
      actionInfo.setResult("Illegal container file path: " + containerFilePath);
      return ScheduleResult.FAIL;
    }
    if (!containerFileCache.contains(containerFilePath)) {
      LOG.debug("{} is not container file.", containerFilePath);
      actionInfo.setResult(containerFilePath + " is not container file.");
      return ScheduleResult.FAIL;
    }

    // Check if container file is locked
    if (!containerFileLock.contains(containerFilePath)) {
      // Get small file list of the container file
      List<String> smallFileList;
      try {
        smallFileList = metaStore.getSmallFilesByContainerFile(containerFilePath);
      } catch (MetaStoreException e) {
        String errMsg = String.format(
            "Failed to get small files of the container file %s for %s.",
            containerFilePath, e.toString());
        LOG.error(errMsg);
        actionInfo.setResult(errMsg);
        return ScheduleResult.FAIL;
      }

      if (!smallFileList.isEmpty()) {
        // Update container file and uncompact small file lock
        containerFileLock.add(containerFilePath);

        // Put small files into arguments of this action
        Map<String, String> args = new HashMap<>(2);
        args.put(HdfsAction.FILE_PATH, new Gson().toJson(smallFileList));
        args.put(SmallFileUncompactAction.CONTAINER_FILE,
            actionInfo.getArgs().get(SmallFileUncompactAction.CONTAINER_FILE));
        action.setArgs(args);
        actionInfo.setArgs(args);
        afterSchedule(actionInfo);
        return ScheduleResult.SUCCESS;
      } else {
        actionInfo.setResult("All the small files of" +
            " this container file already be uncompacted.");
        actionInfo.setSuccessful(true);
        return ScheduleResult.SUCCESS;
      }
    } else {
      // Retry if container file is locked
      return ScheduleResult.RETRY;
    }
  }

  @Override
  public ScheduleResult onSchedule(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      LaunchCmdlet cmdlet, LaunchAction action, int actionIndex) {
    if (COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      return getCompactScheduleResult(actionInfo);
    } else if (UNCOMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      return getUncompactScheduleResult(actionInfo, action);
    }
    return ScheduleResult.SUCCESS;
  }

  /**
   * Speculate action status and set result accordingly.
   */
  @Override
  public boolean isSuccessfulBySpeculation(ActionInfo actionInfo) {
    try {
      boolean isSuccessful = true;
      List<FileState> fileStateList = new ArrayList<>();
      // If any one small file is not compacted, return false.
      for (String path : getSmallFileList(actionInfo)) {
        FileState fileState = HadoopUtil.getFileState(dfsClient, path);
        FileState.FileType fileType = fileState.getFileType();
        if (!isExpectedFileState(fileType, actionInfo.getActionName())) {
          isSuccessful = false;
          break;
        }
        // Only add compact file state.
        if (actionInfo.getActionName().equals(COMPACT_ACTION_NAME)) {
          fileStateList.add(fileState);
        }
      }
      if (!isSuccessful) {
        return false;
      }
      if (actionInfo.getActionName().equals(UNCOMPACT_ACTION_NAME)) {
        return true;
      }
      // Recover action result for successful compact action.
      if (actionInfo.getActionName().equals(COMPACT_ACTION_NAME)) {
        List<CompactFileState> compactFileStates = new ArrayList<>();
        assert fileStateList.size() == getSmallFileList(actionInfo).size();
        for (FileState fileState : fileStateList) {
          compactFileStates.add((CompactFileState) fileState);
        }
        actionInfo.setResult(new Gson().toJson(compactFileStates));
      }
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to get file state, suppose this action was not " +
          "successfully executed: {}", actionInfo.toString());
      return false;
    }
  }

  public boolean isExpectedFileState(FileState.FileType fileType,
      String actionName) {
    if (actionName.equals(COMPACT_ACTION_NAME)) {
      return fileType == FileState.FileType.COMPACT;
    }
    return fileType == FileState.FileType.NORMAL;
  }

  /**
   * Do something after a successful scheduling.
   * For compact/uncompact action, the original small file will be replaced by
   * other file with new fid. We need to keep the original file's id to let new
   * file take over its data temperature metric.
   */
  public void afterSchedule(ActionInfo actionInfo) {
    try {
      // Set old file ID, which will be persisted to DB.
      setOldFileId(actionInfo);
    } catch (Throwable t) {
      // We think it may not be a big issue, so just warn user this issue.
      LOG.warn("Failed in maintaining old fid for taking over " +
          "old data's temperature.");
    }
  }

  /**
   * Set old file id which will be persisted into DB. For action status
   * recovery case, the old file id can be acquired for taking over old file's
   * data temperature.
   */
  private void setOldFileId(ActionInfo actionInfo) throws IOException {
    if (actionInfo.getArgs().get(OLD_FILE_ID) != null &&
        !actionInfo.getArgs().get(OLD_FILE_ID).isEmpty()) {
      return;
    }
    List<Long> oids = new ArrayList<>();
    // For uncompact, small file list will be set by #onSchedule.
    for (String path : getSmallFileList(actionInfo)) {
      try {
        oids.add(dfsClient.getFileInfo(path).getFileId());
      } catch (IOException e) {
        LOG.warn("Failed to set old fid for taking over data temperature!");
        throw e;
      }
    }
    actionInfo.setOldFileIds(oids);
  }

  /**
   * Handle compact action result.
   */
  private void handleCompactActionResult(ActionInfo actionInfo) {
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

    // Update container file cache, compact file state queue,
    // handling small file cache
    if (compactFileStates != null && !compactFileStates.isEmpty()) {
      LOG.debug(String.format("Add container file %s into cache.",
          containerFilePath));
      containerFileCache.add(containerFilePath);
      for (CompactFileState compactFileState : compactFileStates) {
        handlingSmallFileCache.add(compactFileState.getPath());
        compactFileStateQueue.offer(compactFileState);
      }
    }

    // Remove locks of container file and small files
    containerFileLock.remove(containerFilePath);
    compactSmallFileLock.removeAll(smallFileList);
  }

  /**
   * Handle uncompact action result.
   */
  private void handleUncompactActionResult(ActionInfo actionInfo) {
    // Get container file path, small files, result of this action
    String containerFilePath = actionInfo.getArgs().get(
        SmallFileUncompactAction.CONTAINER_FILE);

    if (actionInfo.isSuccessful()) {
      containerFileCache.remove(containerFilePath);
    }

    // Remove locks of container file
    containerFileLock.remove(containerFilePath);
  }

  @Override
  public void onActionFinished(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      int actionIndex) {
    if (!actionInfo.getActionName().equals(COMPACT_ACTION_NAME) &&
        !actionInfo.getActionName().equals(UNCOMPACT_ACTION_NAME)) {
      return;
    }
    if (!actionInfo.isFinished()) {
      return;
    }
    if (COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      handleCompactActionResult(actionInfo);
    } else if (UNCOMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      handleUncompactActionResult(actionInfo);
    }
    if (actionInfo.isSuccessful()) {
      // For uncompact action, the small file list cannot be obtained from metastore,
      // since the record can be deleted because container file was deleted.
      takeOverAccessCount(actionInfo);
    }
  }

  public List<String> getSmallFileList(ActionInfo actionInfo) {
    return new Gson().fromJson(actionInfo.getArgs().get(HdfsAction.FILE_PATH),
        new TypeToken<ArrayList<String>>() {
        }.getType());
  }

  /**
   * In rename case, the fid of renamed file is not changed. But sometimes, we need
   * to keep old file's access count and let new file takes over this metric. E.g.,
   * with (un)EC/(de)Compress/(un)Compact action, a new file will overwrite the old file.
   */
  public void takeOverAccessCount(ActionInfo actionInfo) {
    List<String> smallFiles = getSmallFileList(actionInfo);
    List<Long> oldFids = actionInfo.getOldFileIds();
    try {
      for (int i = 0; i < smallFiles.size(); i++) {
        String filePath = smallFiles.get(i);
        long oldFid = oldFids.get(i);
        // The new fid may have not been updated in metastore, so
        // we get it from dfs client.
        long newFid = dfsClient.getFileInfo(filePath).getFileId();
        metaStore.updateAccessCountTableFid(oldFid, newFid);
      }
    } catch (Exception e) {
      LOG.warn("Failed to take over file access count, which can make the " +
          "measure for data temperature inaccurate!", e);
    }
  }

  /**
   * Sync compact file states with meta store.
   */
  private void syncMetaStore() {
    List<CompactFileState> compactFileStates = new ArrayList<>();

    // Get compact file states from compactFileStateQueue
    for (int i = 0; i < META_STORE_INSERT_BATCH_SIZE; i++) {
      CompactFileState compactFileState = compactFileStateQueue.poll();
      if (compactFileState != null) {
        try {
          FileInfo info = metaStore.getFile(compactFileState.getPath());
          if (info != null && info.getLength() == 0) {
            LOG.debug(String.format("Ready to insert the file state of %s.",
                compactFileState.getPath()));
            compactFileStates.add(compactFileState);
          } else {
            LOG.debug(String.format(
                "Waiting for the small file %s synced in the meta store.",
                compactFileState.getPath()));
            compactFileStateQueue.offer(compactFileState);
          }
        } catch (MetaStoreException e) {
          LOG.error("Failed to get file info.", e);
          compactFileStateQueue.offer(compactFileState);
        }
      } else {
        break;
      }
    }

    // Batch insert compact file states into meta store
    try {
      if (!compactFileStates.isEmpty()) {
        metaStore.insertCompactFileStates(
            compactFileStates.toArray(new CompactFileState[0]));
        for (CompactFileState fileState : compactFileStates) {
          handlingSmallFileCache.remove(fileState.getPath());
        }
      }
    } catch (MetaStoreException e) {
      for (CompactFileState fileState : compactFileStates) {
        handlingSmallFileCache.remove(fileState.getPath());
      }
      LOG.error("Failed to update file state of meta store.", e);
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
