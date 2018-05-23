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
   * compact small files lock.
   */
  private List<String> compactSmallFilesLock;

  /**
   * uncompact small files lock.
   */
  private List<String> uncompactSmallFilesLock;

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
    this.containerFilesLock = new ConcurrentHashMap<>();
    this.compactSmallFilesLock = Collections.synchronizedList(new ArrayList<String>());
    this.uncompactSmallFilesLock = Collections.synchronizedList(new ArrayList<String>());
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
        LOG.error("Illegal container file path: " + containerFilePath);
        return false;
      }

      // Check if small file list is null or empty
      String smallFiles = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      if (smallFiles == null || smallFiles.isEmpty()) {
        LOG.error("Illegal small files: " + smallFiles);
        return false;
      }
      return true;
    } else if (UNCOMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Check if container file is exist
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileUncompactAction.CONTAINER_FILE);
      try {
        if (containerFilePath == null
            || containerFilePath.isEmpty()
            || !dfsClient.exists(containerFilePath)) {
          LOG.error("Illegal container file path: " + containerFilePath);
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
      if (containerFilesLock.containsKey(containerFilePath)) {
        return ScheduleResult.RETRY;
      } else {
        // Check if small file path is valid and unlocked
        for (String smallFile : smallFileList) {
          if (smallFile == null || smallFile.isEmpty()) {
            LOG.error("Illegal small file path: " + smallFile);
            return ScheduleResult.FAIL;
          } else if (compactSmallFilesLock.contains(smallFile)) {
            LOG.error("The small file is locked: " + smallFile);
            return ScheduleResult.FAIL;
          }
        }

        // Check if small file is NORMAL
        for (String smallFile : smallFileList) {
          try {
            FileState smallFileState = metaStore.getFileState(smallFile);
            FileState.FileType smallFileType = smallFileState.getFileType();
            if (smallFileType != FileState.FileType.NORMAL) {
              LOG.error(String.format(
                  "%s has invalid file state %s for small file compact.",
                  smallFile, smallFileState.getFileType().toString()));
              return ScheduleResult.FAIL;
            }
          } catch (MetaStoreException e) {
            LOG.error("Failed to get file state of: " + smallFile);
            return ScheduleResult.FAIL;
          }
        }

        try {
          // Lock container file and set whether exists already
          // Lock small files
          boolean exist = dfsClient.exists(containerFilePath);
          containerFilesLock.put(containerFilePath, exist);
          compactSmallFilesLock.addAll(smallFileList);
        } catch (IOException e) {
          LOG.error("Failed to check if the container file is exists: "
              + containerFilePath, e);
          return ScheduleResult.FAIL;
        }
      }

      return ScheduleResult.SUCCESS;
    } else if (UNCOMPACT_ACTION_NAME.equals(actionInfo.getActionName())) {
      // Get container file and small file list of this action
      String containerFilePath = actionInfo.getArgs().get(
          SmallFileCompactAction.CONTAINER_FILE);

      // Check if container file is locked
      if (!containerFilesLock.containsKey(containerFilePath)) {
        // Get small files of the container file
        List<String> smallFileList;
        try {
          smallFileList = metaStore.getSmallFilesByContainerFile(containerFilePath);
        } catch (MetaStoreException e) {
          LOG.error("Failed to get small files of the container file: "
              + containerFilePath, e);
          return ScheduleResult.FAIL;
        }

        if (smallFileList != null && !smallFileList.isEmpty()) {
          // Check if small file path is valid and unlocked
          for (String smallFile : smallFileList) {
            if (smallFile == null || smallFile.isEmpty()) {
              return ScheduleResult.FAIL;
            } else if (uncompactSmallFilesLock.contains(smallFile)) {
              LOG.error("The small file is locked: " + smallFile);
              return ScheduleResult.FAIL;
            }
          }

          // Update container files and uncompact small files lock
          containerFilesLock.put(containerFilePath, true);
          uncompactSmallFilesLock.addAll(smallFileList);

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
        // Retry if locked
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

        // Insert file states into meta store
        for (CompactFileState compactFileState : compactFileStates) {
          try {
            metaStore.insertUpdateFileState(compactFileState);
          } catch (MetaStoreException e1) {
            LOG.error(String.format(
                "Failed to insert compact file state of %s for %s ",
                compactFileState.getPath(), e1.toString()));
          }
        }

        if (compactFileStates.size() == 0 || !actionInfo.isSuccessful()) {
          try {
            if (!containerFilesLock.get(containerFilePath)
                && dfsClient.getFileInfo(containerFilePath).getLen() == 0) {
              // Delete container file if not exists before running action,
              // and no small file compacted to it
              dfsClient.delete(containerFilePath, false);
            }
          } catch (IOException e2) {
            LOG.error("Failed to handle container file: "
                + containerFilePath, e2);
          }
        }

        // Remove locks of container file and small files
        containerFilesLock.remove(containerFilePath);
        compactSmallFilesLock.removeAll(smallFileList);
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

        // Remove small file states from the meta store
        for (String removeSmallFile : removeSmallFiles) {
          try {
            metaStore.deleteFileState(removeSmallFile);
          } catch (MetaStoreException e1) {
            LOG.error(String.format(
                "Failed to remove small file state of %s from meta store for %s ",
                removeSmallFile, e1.toString()));
          }
        }

        if (removeSmallFiles.size() > 0) {
          if (actionInfo.isSuccessful()) {
            try {
              // Delete container file if action is successful
              dfsClient.delete(containerFilePath, false);
            } catch (IOException e2) {
              LOG.error("Failed to delete container file: "
                  + containerFilePath, e2);
            }
          } else {
            try {
              List<String> list = metaStore.getSmallFilesByContainerFile(containerFilePath);
              if (list == null || list.isEmpty()) {
                // Delete container file if the container file does not have any small file
                dfsClient.delete(containerFilePath, false);
              }
            } catch (MetaStoreException e3) {
              LOG.error("Failed to get small files of the container file: "
                  + containerFilePath, e3);
            } catch (IOException e4) {
              LOG.error("Failed to delete container file: "
                  + containerFilePath, e4);
            }
          }
        }

        // Remove locks of container file and small files
        containerFilesLock.remove(containerFilePath);
        uncompactSmallFilesLock.removeAll(smallFileList);
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
