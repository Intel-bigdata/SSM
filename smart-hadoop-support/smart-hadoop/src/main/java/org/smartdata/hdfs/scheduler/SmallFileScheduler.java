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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.metastore.ActionSchedulerService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SmallFileScheduler extends ActionSchedulerService {
  private MetaStore metaStore;
  private List<String> fileLock;
  private String containerFile = null;
  private Map<String, FileContainerInfo> fileContainerInfoMap;
  public static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore) throws IOException {
    super(context, metaStore);
    this.metaStore = metaStore;
  }

  @Override
  public void init() throws IOException {
    this.fileLock = new ArrayList<>();
    this.fileContainerInfoMap = new ConcurrentHashMap<>();
  }

  private static final List<String> actions = Arrays.asList("write", "read", "compact");

  public List<String> getSupportedActions() {
    return actions;
  }

  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (actionInfo.getActionName().equals("compact")) {
      try {
        // Check container file is null
        String containerFilePath = action.getArgs().get("-containerFile");
        if (containerFilePath == null) {
          return ScheduleResult.FAIL;
        } else {
          this.containerFile = containerFilePath;
        }

        if (fileLock.contains(containerFile)) {
          LOG.error("This container file: " + containerFile + " is locked.");
          return ScheduleResult.RETRY;
        } else {
          fileLock.add(containerFile); // Lock this container file
        }

        // Get file container info of small files
        long offset = 0L;
        String smallFiles = action.getArgs().get("-smallFiles");
        ArrayList<String> smallFileList = new Gson().fromJson(smallFiles, new ArrayList<String>().getClass());
        for (String filePath : smallFileList) {
          FileInfo fileInfo = metaStore.getFile(filePath);
          long fileLen = fileInfo.getLength();
          fileContainerInfoMap.put(filePath, new FileContainerInfo(containerFile, offset, fileLen));
          offset += fileLen;
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
        if (actionInfo.getActionName().equals("compact")) {
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
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }
}
