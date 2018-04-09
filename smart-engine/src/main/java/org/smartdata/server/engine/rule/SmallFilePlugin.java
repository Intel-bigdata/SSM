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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SmallFilePlugin implements RuleExecutorPlugin {
  private MetaStore metaStore;
  private static final int BATCH_SIZE = 200;
  private static final String COMPACT_SYMBOL = "compact";
  private static final String CONTAINER_PREFIX = "/container_files/container_file_";
  private static final Logger LOG = LoggerFactory.getLogger(SmallFilePlugin.class);

  public SmallFilePlugin(MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  @Override
  public void onNewRuleExecutor(final RuleInfo ruleInfo, TranslateResult tResult) {}

  @Override
  public boolean preExecution(final RuleInfo ruleInfo, TranslateResult tResult) {
    return true;
  }

  @Override
  public List<String> preSubmitCmdlet(final RuleInfo ruleInfo, List<String> objects) {
    if (ruleInfo.getRuleText().contains(COMPACT_SYMBOL)) {
      if (objects == null) {
        return null;
      }

      // Get valid small file lists according to the file permission
      List<List<String>> validFileLists = new ArrayList<>();
      try {
        List<String> validFiles = getValidFileList(objects);
        while (!validFiles.isEmpty()) {
          Iterator<String> iterator = validFiles.iterator();
          String first = iterator.next();
          iterator.remove();
          List<String> listElement = new ArrayList<>();
          listElement.add(first);
          FileInfo fileInfoFirst = metaStore.getFile(first);
          while (iterator.hasNext()) {
            String temp = iterator.next();
            FileInfo fileInfo = metaStore.getFile(temp);
            if (checkPermissions(fileInfoFirst, fileInfo)) {
              listElement.add(temp);
              iterator.remove();
            }
          }
          validFileLists.add(listElement);
        }
      } catch (MetaStoreException e) {
        LOG.error("Failed to get valid small files.", e);
      }

      // Split small files according to the batch size
      List<String> smallFileList = new ArrayList<>(200);
      for (List<String> listElement : validFileLists) {
        int size = listElement.size();
        for (int i = 0; i < size; i += BATCH_SIZE) {
          int toIndex = (i + BATCH_SIZE <= size) ? i + BATCH_SIZE : size;
          String smallFiles = new Gson().toJson(listElement.subList(i, toIndex));
          smallFileList.add(smallFiles);
        }
      }

      return smallFileList;
    } else {
      return objects;
    }
  }

  private List<String> getValidFileList(List<String> objects) throws MetaStoreException {
    List<String> fileList  = new ArrayList<>();
    for (String object : objects) {
      long fileLen  = metaStore.getFile(object).getLength();
      FileState fileState = metaStore.getFileState(object);
      FileState.FileType fileType = fileState.getFileType();
      FileState.FileStage fileStage = fileState.getFileStage();
      if (fileLen > 0 && fileType.equals(FileState.FileType.NORMAL)
          && fileStage.equals(FileState.FileStage.DONE)) {
        fileList.add(object);
      }
    }
    return fileList;
  }

  private boolean checkPermissions(FileInfo checkInfo, FileInfo checkedInfo) {
    return (checkInfo.getPermission() == checkedInfo.getPermission())
        && checkInfo.getOwner().equals(checkedInfo.getOwner())
        && checkInfo.getGroup().equals(checkedInfo.getGroup());
  }

  @Override
  public CmdletDescriptor preSubmitCmdletDescriptor(
      final RuleInfo ruleInfo, TranslateResult tResult, CmdletDescriptor descriptor) {
    for (int i = 0; i < descriptor.actionSize(); i++) {
      if (COMPACT_SYMBOL.equals(descriptor.getActionName(i))) {
        Map<String, String> args = descriptor.getActionArgs(i);
        String smallFileList = args.get(HdfsAction.FILE_PATH);
        if (smallFileList != null) {
          String containerFile = args.get(SmallFileCompactAction.CONTAINER_FILE);
          if (containerFile == null) {
            try {
              containerFile = getContainerFile();
            } catch (MetaStoreException e) {
              LOG.error("Failed to generate a new container file.", e);
            }
          }
          descriptor.addActionArg(i, SmallFileCompactAction.CONTAINER_FILE, containerFile);
        }
      }
    }
    return descriptor;
  }

  @Override
  public void onRuleExecutorExit(final RuleInfo ruleInfo) {
  }

  private String getContainerFile() throws MetaStoreException {
    while (true) {
      int random = Math.abs(new Random().nextInt());
      String genContainerFile = CONTAINER_PREFIX + random;
      if (metaStore.getFile(genContainerFile) == null) {
        return genContainerFile;
      }
    }
  }
}
