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
  private Map<String, ContainerFileInfo> containerFileCache;
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
    this.smallFileInfoMap = new ConcurrentHashMap<>();
    this.containerFileCache = new ConcurrentHashMap<>();
    initializeContainerFileCache();
  }

  /**
   * Initialize container file cache.
   */
  private void initializeContainerFileCache() {
    try {
      List<String> containerFiles = metaStore.getAllContainerFiles();
      List<FileInfo> fileInfos = metaStore.getFilesByPaths(containerFiles);
      for (FileInfo fileInfo : fileInfos) {
        if (fileInfo.getLength() < containerFileSizeThreshold) {
          containerFileCache.put(fileInfo.getPath(),
              new ContainerFileInfo(fileInfo));
        }
      }
    } catch (MetaStoreException e) {
      LOG.error("Failed to get file info of all the container files.", e);
    }
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
      if (objects == null || objects.isEmpty()) {
        LOG.debug("Objects is null or empty.");
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
            SmartFilePermission filePermission = new SmartFilePermission(fileInfo);
            if (filePermissionMap.containsKey(filePermission)) {
              filePermissionMap.get(filePermission).add(object);
            } else {
              smallFileInfoMap.put(object, fileInfo);
              List<String> list = new ArrayList<>();
              list.add(object);
              filePermissionMap.put(filePermission, list);
            }
          } else {
            LOG.debug("Invalid file {} for small file compact.", object);
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

          // Get the first small file info
          String firstFile = smallFileList.get(0);
          FileInfo firstFileInfo;
          if (smallFileInfoMap.containsKey(firstFile)) {
            firstFileInfo = smallFileInfoMap.get(firstFile);
          } else {
            try {
              firstFileInfo = metaStore.getFile(firstFile);
              if (firstFileInfo == null) {
                LOG.debug("{} is not exist!!!", firstFile);
                continue;
              }
            } catch (MetaStoreException e) {
              LOG.error("Failed to get file info of: " + firstFile, e);
              continue;
            }
          }

          // Get valid compact action arguments
          SmartFilePermission firstFilePermission = new SmartFilePermission(
              firstFileInfo);
          String firstFileDir = firstFile.substring(0, firstFile.lastIndexOf("/") + 1);
          CompactActionArgs args = getCompactActionArgs(firstFileDir,
              firstFilePermission, smallFileList);

          // Set container file path and its permission, file path of this action
          descriptor.addActionArg(
              i, SmallFileCompactAction.CONTAINER_FILE, args.containerFile);
          descriptor.addActionArg(
              i, SmallFileCompactAction.CONTAINER_FILE_PERMISSION,
              new Gson().toJson(args.containerFilePermission));
          descriptor.addActionArg(
              i, HdfsAction.FILE_PATH, new Gson().toJson(args.smartFiles));
        }
      }
    }
    return descriptor;
  }

  /**
   * Construct compact action arguments.
   */
  private class CompactActionArgs {
    private String containerFile;
    private SmartFilePermission containerFilePermission;
    private List<String> smartFiles;

    private CompactActionArgs(String containerFile,
        SmartFilePermission containerFilePermission, List<String> smartFiles) {
      this.containerFile = containerFile;
      this.containerFilePermission = containerFilePermission;
      this.smartFiles = smartFiles;
    }
  }

  /**
   * Get valid compact action arguments.
   */
  private CompactActionArgs getCompactActionArgs(String firstFileDir,
      SmartFilePermission firstFilePermission, List<String> smallFileList) {
    if (!containerFileCache.isEmpty()) {
      for (Map.Entry<String, ContainerFileInfo> entry
          : containerFileCache.entrySet()) {
        String containerFile = entry.getKey();
        String containerFileDir = containerFile.substring(
            0, containerFile.lastIndexOf("/") + 1);
        SmartFilePermission containerFilePermission =
            entry.getValue().smartFilePermission;
        if (firstFileDir.equals(containerFileDir)
            && firstFilePermission.equals(containerFilePermission)) {
          List<String> validSmallFiles;
          try {
            validSmallFiles = getValidSmallFiles(containerFile, smallFileList);
          } catch (MetaStoreException e) {
            return genCompactActionArgs(firstFileDir,
                firstFilePermission, smallFileList);
          }
          if (validSmallFiles != null) {
            return new CompactActionArgs(containerFile, null, validSmallFiles);
          }
        }
      }
    }
    return genCompactActionArgs(firstFileDir,
        firstFilePermission, smallFileList);
  }

  /**
   * Generate new compact action arguments based on first file info.
   */
  private CompactActionArgs genCompactActionArgs(String firstFileDir,
      SmartFilePermission firstFilePermission, List<String> smallFileList) {
    // Generate new container file
    String containerFile = firstFileDir + CONTAINER_FILE_PREFIX
        + UUID.randomUUID().toString().replace("-", "");
    containerFileCache.put(containerFile,
        new ContainerFileInfo(0, firstFilePermission));
    return new CompactActionArgs(containerFile,
        firstFilePermission, smallFileList);
  }

  /**
   * Get valid small files according to container file.
   */
  private List<String> getValidSmallFiles(String containerFile,
      List<String> smallFileList) throws MetaStoreException {
    // Get container file len
    FileInfo containerFileInfo;
    long containerFileLen;
    try {
      containerFileInfo = metaStore.getFile(containerFile);
    } catch (MetaStoreException e) {
      LOG.error("Failed to get container file length: " + containerFile);
      throw e;
    }
    if (containerFileInfo == null) {
      containerFileCache.remove(containerFile);
      return null;
    } else {
      containerFileLen = containerFileInfo.getLength();
    }

    // Get small files can be compacted to container file
    List<String> ret = new ArrayList<>();
    List<FileInfo> smallFileInfos = metaStore.getFilesByPaths(smallFileList);
    for (FileInfo fileInfo : smallFileInfos) {
      containerFileLen += fileInfo.getLength();
      if (containerFileLen < containerFileSizeThreshold * 1.2) {
        ret.add(fileInfo.getPath());
      }
    }

    if (!ret.isEmpty()) {
      // Update container file map
      if (containerFileLen > containerFileSizeThreshold) {
        containerFileCache.remove(containerFile);
      } else {
        SmartFilePermission filePermission = containerFileCache.get(
            containerFile).smartFilePermission;
        containerFileCache.put(containerFile,
            new ContainerFileInfo(containerFileLen, filePermission));
      }
      return ret;
    }
    return null;
  }

  /**
   * Handle container file info.
   */
  private class ContainerFileInfo {
    private long length;
    private SmartFilePermission smartFilePermission;

    private ContainerFileInfo(long length,
        SmartFilePermission filePermission) {
      this.length = length;
      this.smartFilePermission = filePermission;
    }

    private ContainerFileInfo(FileInfo fileInfo) {
      this.length = fileInfo.getLength();
      this.smartFilePermission = new SmartFilePermission(fileInfo);
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

  @Override
  public void onRuleExecutorExit(final RuleInfo ruleInfo) {
  }
}
