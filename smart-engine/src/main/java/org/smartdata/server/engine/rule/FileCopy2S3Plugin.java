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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.action.Copy2S3Action;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.utils.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileCopy2S3Plugin implements RuleExecutorPlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileCopy2S3Plugin.class.getName());
  private List<String> srcBases;

  public FileCopy2S3Plugin() {
    srcBases = null;
  }


  @Override
  public void onNewRuleExecutor(RuleInfo ruleInfo,
      TranslateResult tResult) {
    srcBases = new ArrayList<>();
    List<String> pathsCheckGlob = tResult.getGlobPathCheck();
    if (pathsCheckGlob.size() == 0) {
      pathsCheckGlob = Collections.singletonList("/*");
    }
    // Get src base list
    srcBases = getPathMatchesList(pathsCheckGlob);
    LOG.debug("Source base list = {}", srcBases);
  }

  private List<String> getPathMatchesList(List<String> paths) {
    List<String> ret = new ArrayList<>();
    for (String p : paths) {
      String dir = StringUtil.getBaseDir(p);
      if (dir == null) {
        continue;
      }
      ret.add(dir);
    }
    return ret;
  }

  @Override
  public boolean preExecution(RuleInfo ruleInfo,
      TranslateResult tResult) {
    return true;
  }

  @Override
  public List<String> preSubmitCmdlet(RuleInfo ruleInfo,
      List<String> objects) {
    return objects;
  }

  @Override
  public CmdletDescriptor preSubmitCmdletDescriptor(RuleInfo ruleInfo,
      TranslateResult tResult, CmdletDescriptor descriptor) {
    for (int i = 0; i < descriptor.getActionSize(); i++) {
      // O(n)
      if (descriptor.getActionName(i).equals("copy2s3")) {
        String srcPath = descriptor.getActionArgs(i).get(Copy2S3Action.SRC);
        String destBase = descriptor.getActionArgs(i).get(Copy2S3Action.DEST);
        String workPath = null;
        // O(n)
        for (String srcBase : srcBases) {
          if (srcPath.startsWith(srcBase)) {
            workPath = srcPath.replaceFirst(srcBase, "");
            break;
          }
        }
        if (workPath == null) {
          LOG.error("Rule {} CmdletDescriptor {} Working Path is empty!", ruleInfo, descriptor);
        }
        // Update dest path
        // dest base + work path = dest full path
        descriptor.addActionArg(i, Copy2S3Action.DEST, destBase + workPath);
      }
    }
    return descriptor;
  }

  @Override
  public void onRuleExecutorExit(RuleInfo ruleInfo) {
    srcBases = null;
  }
}
