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

public class SmallFilePlugin implements RuleExecutorPlugin {
  private int batchSize;
  private MetaStore metaStore;
  private static final String COMPACT_ACTION_NAME = "compact";
  private static final String CONTAINER_FILE_PREFIX = "container_file_";
  private static final Logger LOG = LoggerFactory.getLogger(SmallFilePlugin.class);

  public SmallFilePlugin(ServerContext context) {
    this.metaStore = context.getMetaStore();
    this.batchSize = context.getConf().getInt(
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_KEY,
        SmartConfKeys.SMART_COMPACT_BATCH_SIZE_DEFAULT);
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
          ArrayList<String> smallFileList = new Gson().fromJson(
              smallFiles, new TypeToken<ArrayList<String>>() {
              }.getType());
          try {
            // Get the first small file info of this action
            String firstFilePath = smallFileList.get(0);
            FileInfo firstFileInfo = metaStore.getFile(firstFilePath);

            // Get container file path
            String containerFileDir = firstFilePath.substring(
                0, firstFilePath.lastIndexOf("/") + 1);
            String containerFile = containerFileDir + CONTAINER_FILE_PREFIX
                + UUID.randomUUID().toString().replace("-", "");

            // Get permission info of the container file
            String containerFilePermission = new Gson().toJson(
                new SmartFilePermission(firstFileInfo));

            // Set container file path and permission of this action
            descriptor.addActionArg(
                i, SmallFileCompactAction.CONTAINER_FILE, containerFile);
            descriptor.addActionArg(i,
                SmallFileCompactAction.CONTAINER_FILE_PERMISSION,
                containerFilePermission);
          } catch (MetaStoreException e) {
            LOG.error("Failed to generate meta data of container file. " + e.toString());
          }
        }
      }
    }
    return descriptor;
  }

  @Override
  public void onRuleExecutorExit(final RuleInfo ruleInfo) {
  }
}
