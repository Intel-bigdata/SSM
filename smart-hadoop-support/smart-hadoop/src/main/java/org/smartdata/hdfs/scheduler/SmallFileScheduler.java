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
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
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
   * container files lock, and whether exist in hdfs already.
   */
  private Map<String, Boolean> containerFilesLock;

  /**
   * small files lock.
   */
  private List<String> smallFilesLock;

  private static final String COMPACT_ACTION_NAME = "compact";
  private static final List<String> ACTIONS = Collections.singletonList("compact");
  public static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore)
      throws IOException {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
  }

  @Override
  public void init() throws IOException {
    this.containerFilesLock = new ConcurrentHashMap<>();
    this.smallFilesLock = new ArrayList<>();
    this.dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
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
        return false;
      }

      // Check if small file list is null or empty
      String smallFiles = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      if (smallFiles == null || smallFiles.isEmpty()) {
        return false;
      }

      // Get valid small file list according to the small file lock map
      ArrayList<String> smallFileList = new Gson().fromJson(
          smallFiles, new TypeToken<ArrayList<String>>() {
          }.getType());
      Iterator<String> iterator = smallFileList.iterator();
      while (iterator.hasNext()) {
        String smallFile = iterator.next();
        if (smallFile == null || smallFile.isEmpty()
            || smallFilesLock.contains(smallFile)) {
          iterator.remove();
        }
      }

      if (smallFileList.isEmpty()) {
        return false;
      }
      smallFilesLock.addAll(smallFileList);

      // Reset args of the action
      Map<String, String> args = new HashMap<>(2);
      args.put(SmallFileCompactAction.CONTAINER_FILE,
          actionInfo.getArgs().get(SmallFileCompactAction.CONTAINER_FILE));
      args.put(SmallFileCompactAction.FILE_PATH, new Gson().toJson(smallFileList));
      actionInfo.setArgs(args);
      return true;
    }

    return true;
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Get container file and small file list of this action
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);
      String smallFiles = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      List<String> smallFileList = new Gson().fromJson(
          smallFiles, new TypeToken<ArrayList<String>>() {
          }.getType());

      // Check if container file is locked and retry
      if (containerFilesLock.containsKey(containerFilePath)) {
        return ScheduleResult.RETRY;
      } else {
        try {
          // Lock the container file, and set whether exist already
          boolean exist = dfsClient.exists(containerFilePath);
          containerFilesLock.put(containerFilePath, exist);
        } catch (IOException e) {
          LOG.error("Failed to check if the container file is exists: "
              + containerFilePath, e);
          smallFilesLock.removeAll(smallFileList);
          return ScheduleResult.FAIL;
        }
      }
      return ScheduleResult.SUCCESS;
    } else {
      return ScheduleResult.SUCCESS;
    }
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.isFinished()
        && COMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Get container file path of this action
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);
      List<String> smallFileList = new Gson().fromJson(
          actionInfo.getArgs().get(HdfsAction.FILE_PATH),
          new TypeToken<ArrayList<String>>() {
          }.getType());
      if (actionInfo.isSuccessful()) {
        Map<String, FileContainerInfo> fileContainerInfoMap = new Gson().fromJson(
            actionInfo.getResult(),
            new TypeToken<HashMap<String, FileContainerInfo>>() {
            }.getType());
        for (Map.Entry<String, FileContainerInfo> entry :
            fileContainerInfoMap.entrySet()) {
          CompactFileState compactFileState = new CompactFileState(
              entry.getKey(), entry.getValue());
          // Insert file container info of the small file into meta store
          try {
            metaStore.insertUpdateFileState(compactFileState);
            CompatibilityHelperLoader.getHelper().truncate0(dfsClient, entry.getKey());
          } catch (MetaStoreException | IOException e) {
            containerFilesLock.remove(containerFilePath);
            smallFilesLock.removeAll(smallFileList);
            LOG.error("Failed to update file state of: " + entry.getKey(), e);
            return;
          }
        }
      } else {
        try {
          if (!containerFilesLock.get(containerFilePath)) {
            dfsClient.delete(containerFilePath, false);
          }
        } catch (IOException e) {
          LOG.error("Failed to delete the container file: "
              + containerFilePath, e);
        }
      }

      // Remove locks of container file and small files
      containerFilesLock.remove(containerFilePath);
      smallFilesLock.removeAll(smallFileList);
    }
  }

  @Override
  public void stop() {
  }

  @Override
  public void start() {
  }
}
