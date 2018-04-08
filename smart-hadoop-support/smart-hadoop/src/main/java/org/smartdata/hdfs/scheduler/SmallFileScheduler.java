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
   * The mapping between action and container file.
   */
  private Map<Long, String> containerFileMap;

  /**
   * The mapping between action and file container information.
   */
  private Map<Long, Map<String, FileContainerInfo>> fileContainerInfoMap;

  /**
   * The mapping between container file and offset.
   */
  private Map<String, Long> containerFileOffsetMap;

  /**
   * The mapping between small file and state.
   */
  private Map<String, Long> smallFilesLock;

  /**
   * The mapping between action and small files.
   */
  private Map<Long, ArrayList<String>> smallFilesMap;

  private static final int MIN_BATCH_SIZE = 10;
  private static final int MAX_RETRY_COUNT = 3;
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
    this.containerFileOffsetMap = new ConcurrentHashMap<>(32);
    this.smallFilesLock = new ConcurrentHashMap<>(200);
    this.smallFilesMap = new ConcurrentHashMap<>(32);
    this.dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
  }

  private static final List<String> ACTIONS = Arrays.asList("write", "compact");

  @Override
  public List<String> getSupportedActions() {
    return ACTIONS;
  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) {
    long actionId = actionInfo.getActionId();
    if (ACTIONS.get(1).equals(actionInfo.getActionName())) {
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
      Map<String, Long> tempSmallFiles = new ConcurrentHashMap<>(200);
      for (String smallFile : smallFileList) {
        if (!smallFilesLock.containsKey(smallFile)) {
          tempSmallFiles.put(smallFile, actionId);
        } else {
          return false;
        }
      }

      // Check if the valid number of small files is greater than the min batch size
      if (tempSmallFiles.size() > MIN_BATCH_SIZE) {
        smallFilesLock.putAll(tempSmallFiles);
        containerFileMap.put(actionInfo.getActionId(), containerFilePath);
        smallFilesMap.put(actionId, new ArrayList<>(tempSmallFiles.keySet()));
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
      // Get or set the mapping between container file and offset
      String containerFilePath = containerFileMap.get(actionId);
      long offset = 0L;
      if (containerFileOffsetMap.containsKey(containerFilePath)) {
        offset = containerFileOffsetMap.get(containerFilePath);
      } else {
        containerFileOffsetMap.put(containerFilePath, 0L);
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
      containerFileOffsetMap.put(containerFilePath, offset);

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
    if (actionInfo.isFinished()) {
      long actionId = actionInfo.getActionId();
      if (actionInfo.isSuccessful() && ACTIONS.get(1).equals(actionInfo.getActionName())) {
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
            CompatibilityHelperLoader.getHelper().setLen2Zero(
                dfsClient, entry.getKey());
          } catch (IOException e2) {
            LOG.error("Failed to truncate the small file: " + entry.getKey(), e2);
          }
        }
        LOG.info("Update file container info successfully.");
      }

      // Remove locks of container file and small files
      containerFilesLock.remove(containerFileMap.get(actionId));
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
