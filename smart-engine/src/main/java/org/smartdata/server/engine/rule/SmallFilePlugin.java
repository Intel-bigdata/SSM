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
package org.smartdata.server.engine.rule;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class SmallFilePlugin implements RuleExecutorPlugin {
  private int batchSize;
  private MetaStore metaStore;
  private String containerFileDir;
  private static final String SUPPORTED_ACTION = "compact";
  private static final String CONTAINER_FILE_PREFIX = "container_file_";
  private static final Logger LOG = LoggerFactory.getLogger(SmallFilePlugin.class);

  public SmallFilePlugin(ServerContext context) {
    this.metaStore = context.getMetaStore();
    this.batchSize = context.getConf().getInt(
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_KEY,
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_DEFAULT);
    String containerFileDir = context.getConf().get(
        SmartConfKeys.SMART_COMPACT_CONTAINER_FILE_DIR_KEY,
        SmartConfKeys.SMART_COMPACT_CONTAINER_FILE_DIR_DEFAULT);
    this.containerFileDir = containerFileDir.endsWith("/")
        ? containerFileDir : (containerFileDir + "/");
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
    if (ruleInfo.getRuleText().contains(SUPPORTED_ACTION)) {
      if (objects == null || objects.size() == 0) {
        return objects;
      }

      // Split valid small files according to the file permission
      Map<FilePermission, List<String>> filePermissionMap = new HashMap<>();
      try {
        for (String object : objects) {
          FileInfo fileInfo = metaStore.getFile(object);
          FileState fileState = metaStore.getFileState(object);
          if ((fileInfo.getLength() > 0)
              && fileState.getFileType().equals(FileState.FileType.NORMAL)
              && fileState.getFileStage().equals(FileState.FileStage.DONE)) {
            FilePermission filePermission = new FilePermission(fileInfo);
            if (filePermissionMap.containsKey(filePermission)) {
              filePermissionMap.get(filePermission).add(object);
            } else {
              List<String> list = new ArrayList<>();
              list.add(object);
              filePermissionMap.put(filePermission, list);
            }
          }
        }
      } catch (MetaStoreException e) {
        LOG.error("Failed to get file permission info.", e);
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

  /**
   * An inner class for handling file permission info conveniently.
   */
  private class FilePermission {
    private short permission;
    private String owner;
    private String group;

    private FilePermission(FileInfo fileInfo) {
      this.permission = fileInfo.getPermission();
      this.owner = fileInfo.getOwner();
      this.group = fileInfo.getGroup();
    }

    @Override
    public int hashCode() {
      return permission ^ owner.hashCode() ^ group.hashCode();
    }

    @Override
    public boolean equals(Object filePermission) {
      if (this == filePermission) {
        return true;
      }
      if (filePermission instanceof FilePermission) {
        FilePermission anPermissionInfo = (FilePermission) filePermission;
        return ((this.permission == anPermissionInfo.permission))
            && this.owner.equals(anPermissionInfo.owner)
            && this.group.equals(anPermissionInfo.group);
      }
      return false;
    }
  }

  @Override
  public CmdletDescriptor preSubmitCmdletDescriptor(
      final RuleInfo ruleInfo, TranslateResult tResult, CmdletDescriptor descriptor) {
    for (int i = 0; i < descriptor.getActionSize(); i++) {
      if (SUPPORTED_ACTION.equals(descriptor.getActionName(i))
          && (descriptor.getActionArgs(i).get(HdfsAction.FILE_PATH) != null)
          && !(descriptor.getActionArgs(i).get(HdfsAction.FILE_PATH).isEmpty())) {
          String containerFile = containerFileDir + CONTAINER_FILE_PREFIX
              + UUID.randomUUID().toString().replace("-", "");
          descriptor.addActionArg(i, SmallFileCompactAction.CONTAINER_FILE, containerFile);
      }
    }
    return descriptor;
  }

  @Override
  public void onRuleExecutorExit(final RuleInfo ruleInfo) {
  }
}
