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
package org.smartdata.server.engine.rule;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartFilePermission;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.FileInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.server.engine.ServerContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SmallFilePlugin implements RuleExecutorPlugin {
  private int batchSize;
  private MetaStore metaStore;
  private long containerFileSizeThreshold;
  private Map<String, FileInfo> smallFileInfoMap;
  private Map<String, ContainerFileInfo> containerFileInfoMap;
  private static final String COMPACT_ACTION_NAME = "compact";
  private static final String CONTAINER_FILE_PREFIX = "container_file_";
  private static final Logger LOG = LoggerFactory.getLogger(SmallFilePlugin.class);

  public SmallFilePlugin(ServerContext context) {
    this.metaStore = context.getMetaStore();
    this.batchSize = context.getConf().getInt(
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_KEY,
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_DEFAULT);
    long containerFileThresholdMB = context.getConf().getLong(
        SmartConfKeys.SMART_COMPACT_CONTAINER_FILE_THRESHOLD_MB_KEY,
        SmartConfKeys.SMART_COMPACT_CONTAINER_FILE_THRESHOLD_MB_DEFAULT);
    this.containerFileSizeThreshold = containerFileThresholdMB * 1024 * 1024;
    this.containerFileInfoMap = new ConcurrentHashMap<>();
    this.smallFileInfoMap = new ConcurrentHashMap<>();
  }

  @Override
  public void onNewRuleExecutor(final RuleInfo ruleInfo, TranslateResult tResult) {
  }

  @Override
  public boolean preExecution(final RuleInfo ruleInfo, TranslateResult tResult) {
    return true;
  }

  @Override
  public List<String> preSubmitCmdlet(final RuleInfo ruleInfo, List<String> objects) {
    if (ruleInfo.getRuleText().contains(COMPACT_ACTION_NAME)) {
      if (objects == null || objects.size() == 0) {
        return objects;
      }

      // Split valid small files according to the file permission
      Map<SmartFilePermission, List<String>> filePermissionMap = new HashMap<>();
      for (String object : objects) {
        try {
          FileInfo fileInfo = metaStore.getFile(object);
          FileState fileState = metaStore.getFileState(object);
          if (fileInfo != null && (fileInfo.getLength() > 0)
              && fileState.getFileType().equals(FileState.FileType.NORMAL)
              && fileState.getFileStage().equals(FileState.FileStage.DONE)) {
            smallFileInfoMap.put(object, fileInfo);
            SmartFilePermission filePermission = new SmartFilePermission(fileInfo);
            if (filePermissionMap.containsKey(filePermission)) {
              filePermissionMap.get(filePermission).add(object);
            } else {
              List<String> list = new ArrayList<>();
              list.add(object);
              filePermissionMap.put(filePermission, list);
            }
          }
        } catch (MetaStoreException e) {
          LOG.error(String.format("Failed to get file info of %s.", object), e);
        }
      }

      // Split small files according to the batch size
      List<String> smallFileList = new ArrayList<>();
      for (List<String> listElement : filePermissionMap.values()) {
        int size = listElement.size();
        for (int i = 0; i < size; i += batchSize) {
          int toIndex = (i + batchSize <= size) ? i + batchSize : size;
          String smallFiles = new Gson().toJson(listElement.subList(i, toIndex));
          smallFileList.add(smallFiles);
        }
      }

      return smallFileList;
    } else {
      return objects;
    }
  }

  @Override
  public CmdletDescriptor preSubmitCmdletDescriptor(
      final RuleInfo ruleInfo, TranslateResult tResult, CmdletDescriptor descriptor) {
    for (int i = 0; i < descriptor.getActionSize(); i++) {
      if (COMPACT_ACTION_NAME.equals(descriptor.getActionName(i))) {
        String smallFiles = descriptor.getActionArgs(i).get(HdfsAction.FILE_PATH);
        if (smallFiles != null && !smallFiles.isEmpty()) {
          // Check if small file list is empty
          ArrayList<String> smallFileList = new Gson().fromJson(
              smallFiles, new TypeToken<ArrayList<String>>() {
              }.getType());
          if (smallFileList == null || smallFileList.isEmpty()) {
            continue;
          }

          // Get the first small file info of this action
          String firstFilePath = smallFileList.get(0);
          FileInfo firstFileInfo = smallFileInfoMap.get(firstFilePath);
          SmartFilePermission firstFilePermission = new SmartFilePermission(
              firstFileInfo);
          String firstFileDir = firstFilePath.substring(
              0, firstFilePath.lastIndexOf("/") + 1);

          // Get container file path and final small file list
          String containerFile = null;
          List<String> finalSmallFileList = new ArrayList<>();
          if (!containerFileInfoMap.isEmpty()) {
            for (Map.Entry<String, ContainerFileInfo> entry
                : containerFileInfoMap.entrySet()) {
              String containerFileDir = entry.getKey().substring(
                  0, firstFilePath.lastIndexOf("/") + 1);
              SmartFilePermission containerFilePermission =
                  entry.getValue().smartFilePermission;
              if (firstFileDir.equals(containerFileDir)
                  && firstFilePermission.equals(containerFilePermission)) {
                containerFile = entry.getKey();
              }
            }

            if (containerFile != null) {
              long sumLen;
              try {
                FileInfo containerFileInfo = metaStore.getFile(containerFile);
                if (containerFileInfo != null) {
                  sumLen = containerFileInfo.getLength();
                } else {
                  containerFileInfoMap.remove(containerFile);
                  // Generate new container file for compact
                  updateDescriptor(descriptor, i, firstFileDir, firstFilePermission);
                  continue;
                }
              } catch (MetaStoreException e) {
                LOG.error("Failed to get container file length: " + containerFile);
                // Generate new container file for compact
                updateDescriptor(descriptor, i, firstFileDir, firstFilePermission);
                continue;
              }
              for (String smallFile : smallFileList) {
                sumLen += smallFileInfoMap.get(smallFile).getLength();
                if (sumLen < containerFileSizeThreshold * 1.2) {
                  finalSmallFileList.add(smallFile);
                }
              }

              // Update container file map
              if (sumLen > containerFileSizeThreshold) {
                containerFileInfoMap.remove(containerFile);
              } else {
                containerFileInfoMap.put(containerFile,
                    new ContainerFileInfo(sumLen, firstFilePermission));
              }

              // Set container file path and permission of this action
              descriptor.addActionArg(
                  i, SmallFileCompactAction.CONTAINER_FILE, containerFile);
              descriptor.addActionArg(
                  i, HdfsAction.FILE_PATH, new Gson().toJson(finalSmallFileList));
              continue;
            }
          }

          // Generate new container file for compact
          updateDescriptor(descriptor, i, firstFileDir, firstFilePermission);
        }
      }
    }
    return descriptor;
  }

  /**
   * An inner class for handling container file info conveniently.
   */
  private class ContainerFileInfo {
    private long length;
    private SmartFilePermission smartFilePermission;

    private ContainerFileInfo(long length,
        SmartFilePermission filePermission) {
      this.length = length;
      this.smartFilePermission = filePermission;
    }

    @Override
    public int hashCode() {
      return Long.valueOf(length).hashCode() ^ smartFilePermission.hashCode();
    }

    @Override
    public boolean equals(Object containerFileInfo) {
      if (this == containerFileInfo) {
        return true;
      }
      if (containerFileInfo instanceof ContainerFileInfo) {
        ContainerFileInfo anContainerFileInfo = (ContainerFileInfo) containerFileInfo;
        return (this.length == anContainerFileInfo.length)
            && this.smartFilePermission.equals(anContainerFileInfo.smartFilePermission);
      }
      return false;
    }
  }

  /**
   * Update descriptor's args of compact action.
   */
  private void updateDescriptor(CmdletDescriptor descriptor, int i,
      String containerFileDir, SmartFilePermission containerFilePermission) {
    String containerFile = containerFileDir + CONTAINER_FILE_PREFIX
        + UUID.randomUUID().toString().replace("-", "");

    // Get permission info of the container file
    containerFileInfoMap.put(containerFile,
        new ContainerFileInfo(0, containerFilePermission));

    // Set container file path and permission of this action
    descriptor.addActionArg(
        i, SmallFileCompactAction.CONTAINER_FILE, containerFile);
    descriptor.addActionArg(i,
        SmallFileCompactAction.CONTAINER_FILE_PERMISSION,
        new Gson().toJson(containerFilePermission));
  }

  @Override
  public void onRuleExecutorExit(final RuleInfo ruleInfo) {
  }
}
