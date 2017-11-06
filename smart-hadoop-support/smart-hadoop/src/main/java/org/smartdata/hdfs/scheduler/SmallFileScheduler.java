/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.metastore.ActionSchedulerService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SmallFileScheduler extends ActionSchedulerService {
  private URI nnUri;
  private DFSClient client;
  private MetaStore metaStore;
  private List<String> fileLock;
  private String containerFile = null;
  private Map<String, FileContainerInfo> fileContainerInfoMap;
  public static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore) throws IOException {
    super(context, metaStore);
    this.nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
    this.metaStore = metaStore;
  }

  @Override
  public void init() throws IOException {
    this.fileLock = new ArrayList<>();
    this.fileContainerInfoMap = new ConcurrentHashMap<>();
    this.client = new DFSClient(nnUri, getContext().getConf());
  }

  private static final List<String> actions = Arrays.asList("write", "read", "compact");

  public List<String> getSupportedActions() {
    return actions;
  }

  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (!actionInfo.getActionName().equals("compact")) {
      return ScheduleResult.FAIL;
    }

    try {
      String srcDir = action.getArgs().get(HdfsAction.FILE_PATH);
      if (srcDir == null) {
        LOG.error("File parameter is missing.");
        return ScheduleResult.FAIL;
      }
      if (!client.exists(srcDir)) {
        LOG.error("Source directory doesn't exist!");
        return ScheduleResult.FAIL;
      }

      // Get the container file
      String containerFilePath = action.getArgs().get("-containerFile");
      if (containerFilePath != null) {
        this.containerFile = containerFilePath;
      } else {
        this.containerFile = getContainerFile();
      }
      if (fileLock.contains(containerFile)) {
        LOG.error("This container file: " + containerFile + " is locked.");
        return ScheduleResult.RETRY;
      } else {
        fileLock.add(containerFile); // Lock this container file
      }

      // Get the small file list
      long offset = 0L;
      long size = Long.valueOf(action.getArgs().get("-size"));
      ArrayList<String> smallFileList = new ArrayList<>();
      List<FileInfo> fileInfoList = metaStore.getFilesByPrefix(srcDir);
      for (FileInfo fileInfo : fileInfoList) {
        long fileLen = fileInfo.getLength();
        String filePath = fileInfo.getPath();
        if (fileLen <= size) {
          smallFileList.add(filePath);
          fileContainerInfoMap.put(filePath, new FileContainerInfo(containerFile, offset, fileLen));
          offset += fileLen;
        }
      }

      action.getArgs().put(SmallFileCompactAction.SMALL_FILES, new Gson().toJson(smallFileList));
      action.getArgs().put(SmallFileCompactAction.CONTAINER_FILE, containerFile);
      return ScheduleResult.SUCCESS;
    } catch (Exception e) {
      LOG.error("Exception occurred while processing " + action, e);
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
        try {
          for (Map.Entry<String, FileContainerInfo> entry : fileContainerInfoMap.entrySet()) {
            metaStore.insertSmallFile(entry.getKey(), entry.getValue());
          }
          if (fileLock.contains(containerFile)) {
            fileLock.remove(containerFile); // Remove container file lock
          }
        } catch (MetaStoreException e) {
          LOG.error("Process small file compact action in metaStore failed!", e);
        }
      }
    }
  }

  @Override
  public void stop() throws IOException {}

  @Override
  public void start() throws IOException {}

  /**
   * An the container file path to compacted in.
   */
  private String getContainerFile() {
    // TODO: get container file path if not specified
    return "/small_files/containerFile";
  }
}
