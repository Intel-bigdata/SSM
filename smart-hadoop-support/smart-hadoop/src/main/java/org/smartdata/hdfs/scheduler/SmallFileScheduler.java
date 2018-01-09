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
  // <Container file path, retry number>
  private Map<String, Integer> fileLock;
  // <actionId, container file path>
  private Map<Long, String> containerFileMap;
  // <actionId, file container info map>
  private Map<Long, Map<String, FileContainerInfo>> fileContainerInfoMap;
  // <container file path, lastOffset>
  private Map<String, Long> containerFileOffsetMap;
  public static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore) throws IOException {
    super(context, metaStore);
    this.metaStore = metaStore;
    this.nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
  }

  @Override
  public void init() throws IOException {
    this.fileLock = new ConcurrentHashMap<>();
    this.containerFileMap = new ConcurrentHashMap<>();
    this.fileContainerInfoMap = new ConcurrentHashMap<>();
    this.containerFileOffsetMap = new ConcurrentHashMap<>();
    this.dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
  }

  private static final List<String> actions = Arrays.asList("write", "read", "compact");

  public List<String> getSupportedActions() {
    return actions;
  }

  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    long actionId = actionInfo.getActionId();
    if (actionInfo.getActionName().equals("compact")) {
      try {

        // Check if container file is null
        String containerFilePath = action.getArgs().get("-containerFile");
        if (containerFilePath == null) {
          return ScheduleResult.FAIL;
        } else {
          containerFileMap.put(actionId, containerFilePath);
        }

        long offset = 0L;
        if (containerFileOffsetMap.containsKey(containerFilePath)) {
          offset = containerFileOffsetMap.get(containerFilePath);
        } else {
          containerFileOffsetMap.put(containerFilePath, 0L);
        }

        // Get file container info of small files
        String smallFiles = action.getArgs().get(HdfsAction.FILE_PATH);
        if (smallFiles == null) {
          return ScheduleResult.FAIL;
        }
        ArrayList<String> smallFileList = new Gson().fromJson(smallFiles, new ArrayList<String>().getClass());
        Map<String, FileContainerInfo> fileContainerInfo = new HashMap<>();
        for (String filePath : smallFileList) {
          FileInfo fileInfo = metaStore.getFile(filePath);
          long fileLen = fileInfo.getLength();
          fileContainerInfo.put(filePath, new FileContainerInfo(containerFilePath, offset, fileLen));
          offset += fileLen;
        }
        fileContainerInfoMap.put(actionId, fileContainerInfo);
        containerFileOffsetMap.put(containerFilePath, offset);

        // Check if container file is locked and retry
        if (fileLock.containsKey(containerFilePath)) {
          int retryNum = fileLock.get(containerFilePath);
          if (retryNum > 3) {
            LOG.error("This container file: " + containerFilePath + " is locked, failed.");
            return ScheduleResult.FAIL;
          } else {
            LOG.warn("This container file: " + containerFilePath + " is locked, retrying.");
            fileLock.put(containerFilePath, retryNum + 1);
            return ScheduleResult.RETRY;
          }
        } else {
          fileLock.put(containerFilePath, 0); // Lock this container file
        }

        return ScheduleResult.SUCCESS;
      } catch (Exception e) {
        LOG.error("Exception occurred while processing " + action, e);
        return ScheduleResult.FAIL;
      }
    } else if (actionInfo.getActionName().equals("write")) {
      // TODO: scheduler for write
      return ScheduleResult.SUCCESS;
    } else if (actionInfo.getActionName().equals("read")) {
      // TODO: scheduler for read
      return ScheduleResult.SUCCESS;
    } else {
      LOG.error("Not support this action: " + actionInfo.getActionName());
      return ScheduleResult.FAIL;
    }
  }

  public void postSchedule(ActionInfo actionInfo, ScheduleResult result) {
  }

  public void onPreDispatch(LaunchAction action) {
  }

  public boolean onSubmit(ActionInfo actionInfo) {
    return true;
  }

  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.isFinished()) {
      if (actionInfo.isSuccessful()) {
        long actionId = actionInfo.getActionId();
        if (actionInfo.getActionName().equals("compact")) {
          try {
            for (Map.Entry<String, FileContainerInfo> entry : fileContainerInfoMap.get(actionId).entrySet()) {
              CompactFileState compactFileState = new CompactFileState(entry.getKey(), entry.getValue());
              metaStore.insertUpdateFileState(compactFileState);
              try {
                CompatibilityHelperLoader.getHelper().setLen2Zero(dfsClient, entry.getKey());
              } catch (IOException e1) {
                LOG.error("Failed to truncate the small file: " + entry.getKey(), e1);
              }
            }
            String containerFilePath = containerFileMap.get(actionId);
            if (fileLock.containsKey(containerFilePath)) {
              fileLock.remove(containerFilePath); // Remove container file lock
            }
            LOG.info("Update file container info successfully.");
          } catch (MetaStoreException e2) {
            LOG.error("Process small file compact action in metaStore failed!", e2);
          }
        }
      }
    }
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }
}
