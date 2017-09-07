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
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.BackUpInfo;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.utils.StringUtil;

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
    List<String> pathsCheck;
    String dirs;
    CmdletDescriptor des = tResult.getCmdDescriptor();
    for (int i = 0; i < des.actionSize(); i++) {
      if (des.getActionName(i).equals("sync")) {
        BackUpInfo backUpInfo = new BackUpInfo();
        pathsCheck = tResult.getGlobPathCheck();
        dirs = getPathMatches(pathsCheck);
        backUpInfo.setSrc(dirs);
        String dest = des.getActionArgs(i).get(SyncAction.DEST);
        if (!dest.endsWith("/")) {
          dest += "/";
        }
        backUpInfo.setDest(dest);
        backUpInfo.setPeriod(tResult.getTbScheduleInfo().getEvery());

        des.addActionArgs(i, SyncAction.SRC, dirs);

        synchronized (backups) {
          if (!backups.containsKey(ruleId)) {
            backups.put(ruleId, new LinkedList<BackUpInfo>());
          }
        }

        List<BackUpInfo> infos = backups.get(ruleId);
        synchronized (infos) {
          try {
            metaStore.deleteBackUpInfoById(ruleId);
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

  private String getPathMatches(List<String> paths) {
    StringBuilder sb = new StringBuilder("");
    for (String p : paths) {
      String dir = StringUtil.getBaseDir(p);
      if (dir == null || dir.length() == 0) {
        continue;
      }
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(dir + "/");
    }
    return sb.toString();
  }

  public boolean preExecution(final RuleInfo ruleInfo, TranslateResult tResult) {
    return true;
  }

  public List<String> preSubmitCmdlet(final RuleInfo ruleInfo, List<String> objects) {
    return objects;
  }

  public CmdletDescriptor preSubmitCmdletDescriptor(final RuleInfo ruleInfo, TranslateResult tResult,
      CmdletDescriptor descriptor) {
    String file = descriptor.getCmdletParameter(HdfsAction.FILE_PATH);
    Map<String, String> args;
    for (int i = 0; i < descriptor.actionSize(); i++) {
      if (descriptor.getActionName(i).equals("sync")) {
        args = descriptor.getActionArgs(i);
        String src = args.get(SyncAction.SRC);
        args.put(SyncAction.SRC, file);
        String dest = args.get(SyncAction.DEST);
        String[] paths = src.split(",");
        for (String p : paths) {
          if (file.startsWith(p)) {
            dest += file.replaceFirst(p, "");
            break;
          }
        }
        args.put(SyncAction.DEST, dest);
      }
    }
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
          metaStore.deleteBackUpInfoById(ruleId);
        }
      } catch (MetaStoreException e) {
        LOG.error("Remove backup info error:" + ruleInfo, e);
      }
    }
  }
}
