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
import org.smartdata.action.SyncAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.utils.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FileCopyDrPlugin implements RuleExecutorPlugin {
  private MetaStore metaStore;
  private Map<Long, List<BackUpInfo>> backups = new HashMap<>();
  private static final Logger LOG =
      LoggerFactory.getLogger(FileCopyDrPlugin.class.getName());

  public FileCopyDrPlugin(MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public void onNewRuleExecutor(final RuleInfo ruleInfo, TranslateResult tResult) {
    long ruleId = ruleInfo.getId();
    List<String> pathsCheckGlob = tResult.getGlobPathCheck();
    if (pathsCheckGlob.size() == 0) {
      pathsCheckGlob = Arrays.asList("/*");
    }
    List<String> pathsCheck = getPathMatchesList(pathsCheckGlob);
    String dirs = StringUtil.join(",", pathsCheck);
    CmdletDescriptor des = tResult.getCmdDescriptor();
    for (int i = 0; i < des.getActionSize(); i++) {
      if (des.getActionName(i).equals("sync")) {

        List<String> statements = tResult.getSqlStatements();
        String before = statements.get(statements.size() - 1);
        String after = before.replace(";", " UNION " + referenceNonExists(tResult, pathsCheck));
        statements.set(statements.size() - 1, after);

        BackUpInfo backUpInfo = new BackUpInfo();
        backUpInfo.setRid(ruleId);
        backUpInfo.setSrc(dirs);
        String dest = des.getActionArgs(i).get(SyncAction.DEST);
        if (!dest.endsWith("/")) {
          dest += "/";
          des.addActionArg(i, SyncAction.DEST, dest);
        }
        backUpInfo.setDest(dest);
        backUpInfo.setPeriod(tResult.getTbScheduleInfo().getEvery());

        des.addActionArg(i, SyncAction.SRC, dirs);

        LOG.debug("Rule executor added for sync rule {} src={}  dest={}", ruleInfo, dirs, dest);

        synchronized (backups) {
          if (!backups.containsKey(ruleId)) {
            backups.put(ruleId, new LinkedList<BackUpInfo>());
          }
        }

        List<BackUpInfo> infos = backups.get(ruleId);
        synchronized (infos) {
          try {
            metaStore.deleteBackUpInfo(ruleId);
            // Add base Sync tag
            FileDiff fileDiff = new FileDiff(FileDiffType.BASESYNC);
            fileDiff.setSrc(backUpInfo.getSrc());
            fileDiff.getParameters().put("-dest", backUpInfo.getDest());
            metaStore.insertFileDiff(fileDiff);
            metaStore.insertBackUpInfo(backUpInfo);
            infos.add(backUpInfo);
          } catch (MetaStoreException e) {
            LOG.error("Insert backup info error:" + backUpInfo, e);
          }
        }
        break;
      }
    }
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

  private String referenceNonExists(TranslateResult tr, List<String> dirs) {
    String temp = "SELECT src FROM file_diff WHERE "
        + "state = 0 AND diff_type IN (1,2) AND (%s);";
    String srcs = "src LIKE '" + dirs.get(0) + "%'";
    for (int i = 1; i < dirs.size(); i++) {
      srcs +=  " OR src LIKE '" + dirs.get(i) + "%'";
    }
    return String.format(temp, srcs);
  }

  public boolean preExecution(final RuleInfo ruleInfo, TranslateResult tResult) {
    return true;
  }

  public List<String> preSubmitCmdlet(final RuleInfo ruleInfo, List<String> objects) {
    return objects;
  }

  public CmdletDescriptor preSubmitCmdletDescriptor(
      final RuleInfo ruleInfo, TranslateResult tResult, CmdletDescriptor descriptor) {
    return descriptor;
  }

  public void onRuleExecutorExit(final RuleInfo ruleInfo) {
    long ruleId = ruleInfo.getId();
    List<BackUpInfo> infos = backups.get(ruleId);
    if (infos == null) {
      return;
    }
    synchronized (infos) {
      try {
        if (infos.size() != 0) {
          infos.remove(0);
        }

        if (infos.size() == 0) {
          backups.remove(ruleId);
          metaStore.deleteBackUpInfo(ruleId);
        }
      } catch (MetaStoreException e) {
        LOG.error("Remove backup info error:" + ruleInfo, e);
      }
    }
  }
}
