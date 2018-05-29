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
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.ServerContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SmallFilePlugin implements RuleExecutorPlugin {
  private int batchSize;
  private MetaStore metaStore;
  private CmdletManager cmdletManager;
  private long containerFileSizeThreshold;
  private Map<String, FileInfo> firstFileInfoCache;
  private Map<RuleInfo, Map<String, FileInfo>> containerFileInfoCache;
  private static final String COMPACT_ACTION_NAME = "compact";
  private static final String CONTAINER_FILE_PREFIX = "container_file_";
  private static final Logger LOG = LoggerFactory.getLogger(SmallFilePlugin.class);

  public SmallFilePlugin(ServerContext context, CmdletManager cmdletManager) {
    this.metaStore = context.getMetaStore();
    this.batchSize = context.getConf().getInt(
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_KEY,
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_DEFAULT);
    this.cmdletManager = cmdletManager;
    long containerFileThresholdMB = context.getConf().getLong(
        SmartConfKeys.SMART_COMPACT_CONTAINER_FILE_THRESHOLD_MB_KEY,
        SmartConfKeys.SMART_COMPACT_CONTAINER_FILE_THRESHOLD_MB_DEFAULT);
    this.containerFileSizeThreshold = containerFileThresholdMB * 1024 * 1024;
    this.firstFileInfoCache = new ConcurrentHashMap<>();
    this.containerFileInfoCache = new ConcurrentHashMap<>();
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
      Map<String, FileInfo> containerFileInfoMap = getContainerFileInfos();
      Map<SmallFileStatus, List<String>> smallFileStateMap = new HashMap<>();
      for (String object : objects) {
        if (containerFileInfoMap.containsKey(object)) {
          LOG.debug("{} is container file.", object);
          continue;
        }
        try {
          FileInfo fileInfo = metaStore.getFile(object);
          FileState fileState = metaStore.getFileState(object);
          if (fileInfo != null
              && fileInfo.getLength() > 0
              && fileInfo.getLength() < containerFileSizeThreshold
              && fileState.getFileType().equals(FileState.FileType.NORMAL)
              && fileState.getFileStage().equals(FileState.FileStage.DONE)) {
            SmallFileStatus smallFileStatus = new SmallFileStatus(fileInfo);
            if (smallFileStateMap.containsKey(smallFileStatus)) {
              smallFileStateMap.get(smallFileStatus).add(object);
            } else {
              firstFileInfoCache.put(object, fileInfo);
              List<String> list = new ArrayList<>();
              list.add(object);
              smallFileStateMap.put(smallFileStatus, list);
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
      for (List<String> listElement : smallFileStateMap.values()) {
        int size = listElement.size();
        for (int i = 0; i < size; i += batchSize) {
          int toIndex = (i + batchSize <= size) ? i + batchSize : size;
          String smallFiles = new Gson().toJson(listElement.subList(i, toIndex));
          smallFileList.add(smallFiles);
        }
      }

      // Update container file info cache for preSubmitCmdletDescriptor
      updateContainerFileInfoCache(ruleInfo, containerFileInfoMap);

      return smallFileList;
    } else {
      return objects;
    }
  }

  /**
   * Get container file info map from meta store.
   */
  private Map<String, FileInfo> getContainerFileInfos() {
    Map<String, FileInfo> ret = new LinkedHashMap<>();
    try {
      List<String> containerFiles = metaStore.getAllContainerFiles();
      if (!containerFiles.isEmpty()) {
        List<FileInfo> fileInfos = metaStore.getFilesByPaths(containerFiles);

        // Sort file infos based on the file length
        Collections.sort(fileInfos, new Comparator<FileInfo>(){
          @Override
          public int compare(FileInfo a, FileInfo b) {
            return Long.compare(a.getLength(), b.getLength());
          }
        });

        for (FileInfo fileInfo : fileInfos) {
          ret.put(fileInfo.getPath(), fileInfo);
        }
      }
    } catch (MetaStoreException e) {
      LOG.error("Failed to get file info of all the container files.", e);
    }
    return ret;
  }

  /**
   * Update container file info cache based on containerFileSizeThreshold
   * and cmdlet.
   */
  private void updateContainerFileInfoCache(RuleInfo ruleInfo,
      Map<String, FileInfo> containerFileInfoMap) {
    if (!containerFileInfoMap.isEmpty()) {
      // Remove container file whose size is greater than containerFileSizeThreshold
      for (Map.Entry<String, FileInfo> entry : containerFileInfoMap.entrySet()) {
        if (entry.getValue().getLength() >= containerFileSizeThreshold) {
          containerFileInfoMap.remove(entry.getKey());
        }
      }

      // Remove container file which is being used
      try {
        List<Long> aids = new ArrayList<>();
        List<CmdletInfo> list = cmdletManager.listCmdletsInfo(ruleInfo.getId());
        for (CmdletInfo cmdletInfo : list) {
          if (!CmdletState.isTerminalState(cmdletInfo.getState())) {
            aids.addAll(cmdletInfo.getAids());
          }
        }
        List<ActionInfo> actionInfos = cmdletManager.getActions(aids);
        for (ActionInfo actionInfo : actionInfos) {
          Map<String, String> args = actionInfo.getArgs();
          if (args.containsKey(SmallFileCompactAction.CONTAINER_FILE)) {
            containerFileInfoMap.remove(
                args.get(SmallFileCompactAction.CONTAINER_FILE));
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to get cmdlet and action info.", e);
      }
    }

    containerFileInfoCache.put(ruleInfo, containerFileInfoMap);
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
          if (firstFileInfoCache.containsKey(firstFile)) {
            firstFileInfo = firstFileInfoCache.get(firstFile);
          } else {
            try {
              firstFileInfo = metaStore.getFile(firstFile);
              if (firstFileInfo == null) {
                LOG.debug("{} is not exist!!!", firstFile);
                continue;
              }
            } catch (MetaStoreException e) {
              LOG.error(String.format("Failed to get file info of: %s.", firstFile), e);
              continue;
            }
          }

          // Get valid compact action arguments
          SmartFilePermission firstFilePermission = new SmartFilePermission(
              firstFileInfo);
          String firstFileDir = firstFile.substring(0, firstFile.lastIndexOf("/") + 1);
          CompactActionArgs args = getCompactActionArgs(ruleInfo, firstFileDir,
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
  private CompactActionArgs getCompactActionArgs(RuleInfo ruleInfo, String firstFileDir,
      SmartFilePermission firstFilePermission, List<String> smallFileList) {
    Map<String, FileInfo> containerFileMap = containerFileInfoCache.get(ruleInfo);
    for (Iterator<Map.Entry<String, FileInfo>> iter =
         containerFileMap.entrySet().iterator(); iter.hasNext();) {
      Map.Entry<String, FileInfo> entry = iter.next();
      String containerFilePath = entry.getKey();
      FileInfo containerFileInfo = entry.getValue();
      iter.remove();

      // Get compact action arguments
      String containerFileDir = containerFilePath.substring(
          0, containerFilePath.lastIndexOf("/") + 1);
      if (firstFileDir.equals(containerFileDir)
          && firstFilePermission.equals(
          new SmartFilePermission(containerFileInfo))) {
        List<String> validSmallFiles;
        try {
          validSmallFiles = getValidSmallFiles(containerFileInfo, smallFileList);
        } catch (MetaStoreException e) {
          LOG.error("Failed to get file info of small files.", e);
          continue;
        }
        if (validSmallFiles != null) {
          return new CompactActionArgs(containerFilePath, null, validSmallFiles);
        }
      }
    }

    return genCompactActionArgs(firstFileDir, firstFilePermission, smallFileList);
  }

  /**
   * Generate new compact action arguments based on first file info.
   */
  private CompactActionArgs genCompactActionArgs(String firstFileDir,
      SmartFilePermission firstFilePermission, List<String> smallFileList) {
    // Generate new container file
    String containerFilePath = firstFileDir + CONTAINER_FILE_PREFIX
        + UUID.randomUUID().toString().replace("-", "");
    return new CompactActionArgs(containerFilePath,
        firstFilePermission, smallFileList);
  }

  /**
   * Get valid small files according to container file.
   */
  private List<String> getValidSmallFiles(FileInfo containerFileInfo,
      List<String> smallFileList) throws MetaStoreException {
    // Get container file len
    long containerFileLen = containerFileInfo.getLength();

    // Sort small file list for getting most eligible small files
    List<String> ret = new ArrayList<>();
    List<FileInfo> smallFileInfos = metaStore.getFilesByPaths(smallFileList);
    Collections.sort(smallFileInfos, new Comparator<FileInfo>(){
      @Override
      public int compare(FileInfo a, FileInfo b) {
        return Long.compare(a.getLength(), b.getLength());
      }
    });

    // Get small files can be compacted to container file
    for (FileInfo fileInfo : smallFileInfos) {
      long fileLen = fileInfo.getLength();
      if (fileLen > 0) {
        containerFileLen += fileLen;
        if (containerFileLen < containerFileSizeThreshold * 1.2) {
          ret.add(fileInfo.getPath());
        }
        if (containerFileLen >= containerFileSizeThreshold) {
          break;
        }
      }
    }

    if (!ret.isEmpty()) {
      return ret;
    } else {
      return null;
    }
  }

  /**
   * Handle small file status.
   */
  private class SmallFileStatus {
    private String dir;
    private SmartFilePermission smartFilePermission;

    private SmallFileStatus(FileInfo fileInfo) {
      String path = fileInfo.getPath();
      this.dir = path.substring(0, path.lastIndexOf("/") + 1);
      this.smartFilePermission = new SmartFilePermission(fileInfo);
    }

    @Override
    public int hashCode() {
      return dir.hashCode() ^ smartFilePermission.hashCode();
    }

    @Override
    public boolean equals(Object smallFileStatus) {
      if (this == smallFileStatus) {
        return true;
      }
      if (smallFileStatus instanceof SmallFileStatus) {
        SmallFileStatus anSmallFileStatus = (SmallFileStatus) smallFileStatus;
        return (this.dir.equals(anSmallFileStatus.dir))
            && this.smartFilePermission.equals(anSmallFileStatus.smartFilePermission);
      }
      return false;
    }
  }

  @Override
  public void onRuleExecutorExit(final RuleInfo ruleInfo) {
  }
}
