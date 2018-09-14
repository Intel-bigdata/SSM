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
package org.smartdata.hdfs.scheduler;

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.action.*;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ErasureCodingScheduler extends ActionSchedulerService {
  public static final Logger LOG = LoggerFactory.getLogger(ErasureCodingScheduler.class);
  public static final String ecActionID = "ec";
  public static final String unecActionID = "unec";
  public static final String checkecActionID = "checkec";
  public static final String listecActionID = "listec";
  public static final List<String> actions =
      Arrays.asList(ecActionID, unecActionID, checkecActionID, listecActionID);
  public static final String EC_DIR = "/system/ssm/ec_tmp/";
  public static final String EC_TMP = "-ecTmp";
  private Set<String> fileLock;
  private SmartConf conf;
  private MetaStore metaStore;

  public ErasureCodingScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.conf = context.getConf();
    this.metaStore = metaStore;
  }

  public List<String> getSupportedActions() {
    return actions;
  }

  public void init() throws IOException {
    fileLock = new HashSet<>();
  }

  @Override
  public void start() throws IOException {

  }

  @Override
  public void stop() throws IOException {

  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) throws IOException {
    if (!isECSupported()) {
      throw new IOException(actionInfo.getActionName() +
          " is not supported on " + VersionInfo.getVersion());
    }
    if (!actionInfo.getActionName().equals(listecActionID)) {
      if (actionInfo.getArgs().get(HdfsAction.FILE_PATH) == null) {
        throw new IOException("No src path is given!");
      }
    }
    return true;
  }

  public static boolean isECSupported() {
    String[] parts = VersionInfo.getVersion().split("\\.");
    return Integer.parseInt(parts[0]) == 3;
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (!actions.contains(action.getActionType())) {
      return ScheduleResult.SUCCESS;
    }

    if (actionInfo.getActionName().equals(listecActionID)) {
      return ScheduleResult.SUCCESS;
    }

    String srcPath = action.getArgs().get(HdfsAction.FILE_PATH);
    if (srcPath == null) {
      actionInfo.appendLog("No file is given in this action!");
      return ScheduleResult.FAIL;
    }

    if (actionInfo.getActionName().equals(checkecActionID)) {
      return ScheduleResult.SUCCESS;
    }

    // check file lock merely for ec & unec action
    if (fileLock.contains(srcPath)) {
      return ScheduleResult.FAIL;
    }
    try {
      if (!metaStore.getFile(srcPath).isdir()) {
        // For ec or unec, add ecTmp argument
        String tmpName = createTmpName(action);
        action.getArgs().put(EC_TMP, EC_DIR + tmpName);
        actionInfo.getArgs().put(EC_TMP, EC_DIR + tmpName);
      }
    } catch (MetaStoreException ex) {
      LOG.error("Error occurred for getting file info", ex);
      actionInfo.appendLog(ex.getMessage());
      return ScheduleResult.FAIL;
    }
    // lock the file only if ec or unec action is scheduled
    fileLock.add(srcPath);
    return ScheduleResult.SUCCESS;
  }

  private String createTmpName(LaunchAction action) {
    String path = action.getArgs().get(HdfsAction.FILE_PATH);
    String fileName;
    int index = path.lastIndexOf("/");
    if (index == path.length() - 1) {
      index = path.substring(0, path.length() - 1).indexOf("/");
      fileName = path.substring(index + 1, path.length() - 1);
    } else {
      fileName = path.substring(index + 1, path.length());
    }
    /**
     * The dest tmp file is under EC_DIR and
     * named by fileName, aidxxx and current time in millisecond with "_" separated
     */
    String tmpName = fileName + "_" + "aid" + action.getActionId() +
        "_" + System.currentTimeMillis();
    return tmpName;
  }

  @Override
  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.getActionName().equals(ecActionID) ||
        actionInfo.getActionName().equals(unecActionID)) {
      fileLock.remove(actionInfo.getArgs().get(HdfsAction.FILE_PATH));
    }
  }
}
