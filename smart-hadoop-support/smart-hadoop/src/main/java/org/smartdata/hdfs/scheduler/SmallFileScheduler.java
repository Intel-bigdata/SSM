/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs.scheduler;

import com.google.gson.Gson;
import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileContainerInfo;
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

public class SmallFileScheduler extends ActionSchedulerService {
  private final URI nnUri;
  private DFSClient dfsClient;
  private MetaStore metaStore;

  /**
   * container files lock, and retry number.
   */
  private Map<String, Integer> containerFilesLock;

  /**
   * The mapping between action and container file exist info.
   */
  private Map<Long, ContainerFileExistInfo> containerFileMap;

  /**
   * The mapping between action and file container information.
   */
  private Map<Long, Map<String, FileContainerInfo>> fileContainerInfoMap;

  /**
   * The mapping between small file and action.
   */
  private Map<String, Long> smallFilesLock;

  /**
   * The mapping between action and small files.
   */
  private Map<Long, ArrayList<String>> smallFilesMap;

  private static final int MIN_BATCH_SIZE = 3;
  private static final int MAX_RETRY_COUNT = 3;
  private static final List<String> ACTIONS = Arrays.asList("write", "compact");
  public static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore) throws IOException {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
  }

  @Override
  public void init() throws IOException {
    this.containerFilesLock = new ConcurrentHashMap<>(32);
    this.containerFileMap = new ConcurrentHashMap<>(32);
    this.fileContainerInfoMap = new ConcurrentHashMap<>(32);
    this.smallFilesLock = new ConcurrentHashMap<>(256);
    this.smallFilesMap = new ConcurrentHashMap<>(32);
    this.dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
  }

  @Override
  public List<String> getSupportedActions() {
    return ACTIONS;
  }

  private class ContainerFileExistInfo {
    private String containerFilePath;
    private boolean isExist;

    private ContainerFileExistInfo(String containerFilePath, boolean isExist) {
      this.containerFilePath = containerFilePath;
      this.isExist = isExist;
    }
  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) {
    if (ACTIONS.get(1).equals(actionInfo.getActionName())) {
      long actionId = actionInfo.getActionId();

      // Check if container file is null
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);
      if (containerFilePath == null) {
        return false;
      }

      // Check if small file list is null
      String smallFiles = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      if (smallFiles == null) {
        return false;
      }
      ArrayList<String> smallFileList = new Gson().fromJson(
          actionInfo.getArgs().get(HdfsAction.FILE_PATH),
          new ArrayList<String>().getClass());
      Iterator<String> iterator = smallFileList.iterator();
      Map<String, Long> tempSmallFiles = new ConcurrentHashMap<>(200);
      while (iterator.hasNext()) {
        String smallFile = iterator.next();
        if (!smallFilesLock.containsKey(smallFile)) {
          tempSmallFiles.put(smallFile, actionId);
        } else {
          iterator.remove();
        }
      }

      // Check if the valid number of small files is greater than the min batch size
      if (smallFileList.size() >= MIN_BATCH_SIZE) {
        smallFilesLock.putAll(tempSmallFiles);

        // Update container file map, save the info that whether it already exists
        try {
          if (dfsClient.exists(containerFilePath)) {
            containerFileMap.put(actionId, new ContainerFileExistInfo(containerFilePath, true));
          } else {
            containerFileMap.put(actionId, new ContainerFileExistInfo(containerFilePath, false));
          }
        } catch (IOException e) {
          LOG.error("Failed to check if the container file is exists: " + containerFilePath, e);
          return false;
        }

        smallFilesMap.put(actionId, smallFileList);
        Map<String, String> args = new HashMap<>(2);
        args.put(SmallFileCompactAction.CONTAINER_FILE,
            actionInfo.getArgs().get(SmallFileCompactAction.CONTAINER_FILE));
        args.put(SmallFileCompactAction.FILE_PATH, new Gson().toJson(smallFileList));
        actionInfo.setArgs(args);
        return true;
      } else {
        return false;
      }
    }

    return true;
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    long actionId = actionInfo.getActionId();
    if (ACTIONS.get(1).equals(actionInfo.getActionName())) {
      String containerFilePath = containerFileMap.get(actionId).containerFilePath;

      // Check if container file is locked and retry
      if (containerFilesLock.containsKey(containerFilePath)) {
        int retryNum = containerFilesLock.get(containerFilePath);
        if (retryNum > MAX_RETRY_COUNT) {
          LOG.error(
              "This container file: " + containerFilePath + " is locked, failed.");
          return ScheduleResult.FAIL;
        } else {
          LOG.warn(
              "This container file: " + containerFilePath + " is locked, retrying.");
          containerFilesLock.put(containerFilePath, retryNum + 1);
          return ScheduleResult.RETRY;
        }
      } else {
        // Lock the container file
        containerFilesLock.put(containerFilePath, 0);
      }

      // Get offset of container file
      long offset;
      try {
        FileInfo containerFileInfo = metaStore.getFile(containerFilePath);
        offset = (containerFileInfo == null) ? 0L : containerFileInfo.getLength();
      } catch (MetaStoreException e) {
        LOG.error("Failed to get file info of the container file: " + containerFilePath);
        return ScheduleResult.FAIL;
      }

      // Get file container info of small files
      List<String> smallFileList = smallFilesMap.get(actionId);
      Map<String, FileContainerInfo> fileContainerInfo = new HashMap<>(
          smallFileList.size());
      for (String filePath : smallFileList) {
        try {
          FileInfo fileInfo = metaStore.getFile(filePath);
          long fileLen = fileInfo.getLength();
          fileContainerInfo.put(
              filePath, new FileContainerInfo(containerFilePath, offset, fileLen));
          offset += fileLen;
        } catch (MetaStoreException e) {
          LOG.error("Exception occurred while scheduling " + action, e);
          return ScheduleResult.FAIL;
        }
      }
      fileContainerInfoMap.put(actionId, fileContainerInfo);

      return ScheduleResult.SUCCESS;
    } else if (ACTIONS.get(0).equals(actionInfo.getActionName())) {
      // TODO: scheduler for write
      return ScheduleResult.SUCCESS;
    } else {
      LOG.error("This action not supported: " + actionInfo.getActionName());
      return ScheduleResult.FAIL;
    }
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.isFinished() && ACTIONS.get(1).equals(actionInfo.getActionName())) {
      long actionId = actionInfo.getActionId();
      if (actionInfo.isSuccessful()) {
        for (Map.Entry<String, FileContainerInfo> entry :
            fileContainerInfoMap.get(actionId).entrySet()) {
          CompactFileState compactFileState = new CompactFileState(
              entry.getKey(), entry.getValue());
          try {
            metaStore.insertUpdateFileState(compactFileState);
          } catch (MetaStoreException e1) {
            LOG.error("Process small file compact action in metaStore failed!", e1);
          }
          try {
            CompatibilityHelperLoader.getHelper().truncate0(dfsClient, entry.getKey());
          } catch (IOException e2) {
            LOG.error("Failed to truncate the small file: " + entry.getKey(), e2);
          }
        }
        LOG.info("Update file container info successfully.");
      } else {
        try {
          if (containerFileMap.get(actionId).isExist) {
            dfsClient.delete(containerFileMap.get(actionId).containerFilePath, false);
          }
        } catch (IOException e3) {
          LOG.error("Failed to delete the container file: " + containerFileMap.get(actionId), e3);
        }
      }

      // Remove locks of container file and small files
      containerFilesLock.remove(containerFileMap.get(actionId).containerFilePath);
      for (Map.Entry<String, FileContainerInfo> entry :
          fileContainerInfoMap.get(actionId).entrySet()) {
        smallFilesLock.remove(entry.getKey());
      }
    }
  }

  @Override
  public void stop() {
  }

  @Override
  public void start() {
  }
}
