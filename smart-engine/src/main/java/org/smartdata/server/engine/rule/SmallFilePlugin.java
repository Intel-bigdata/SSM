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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;

import java.util.List;

public class SmallFilePlugin implements RuleExecutorPlugin {
  private MetaStore metaStore;
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

  public List<String> preSubmitCmdlet(final RuleInfo ruleInfo, List<String> objects) {
    // TODO: Get the small file list to compact action
    return objects;
  }

  public CmdletDescriptor preSubmitCmdletDescriptor(
      final RuleInfo ruleInfo, TranslateResult tResult, CmdletDescriptor descriptor) {
    /*for (int i = 0; i < descriptor.actionSize(); i++) {
      if (descriptor.getActionName(i).equals("compact")) {
        Map<String, String> args =  descriptor.getActionArgs(i);
        String srcDir = args.get(HdfsAction.FILE_PATH);
        long size = Long.valueOf(args.get("-size"));

        // Get the small file list
        try {
          ArrayList<String> smallFileList = new ArrayList<>();
          List<FileInfo> fileInfoList = metaStore.getFilesByPrefix(srcDir);
          for (FileInfo fileInfo : fileInfoList) {
            long fileLen = fileInfo.getLength();
            String filePath = fileInfo.getPath();
            if (fileLen <= size) {
              smallFileList.add(filePath);
            }
          }
          if (smallFileList.size() > 0) {
            descriptor.addActionArg(i, SmallFileCompactAction.SMALL_FILES, new Gson().toJson(smallFileList));
            descriptor.deleteActionArg(i, HdfsAction.FILE_PATH);
            descriptor.deleteActionArg(i, "-size");
          } else {
            LOG.debug("No small files in " + srcDir);
          }
        } catch (MetaStoreException e) {
          LOG.error("Failed to get small files list from: " + srcDir, e);
        }
      }
    }*/
    return descriptor;
  }

  public void onRuleExecutorExit(final RuleInfo ruleInfo) {}
}
