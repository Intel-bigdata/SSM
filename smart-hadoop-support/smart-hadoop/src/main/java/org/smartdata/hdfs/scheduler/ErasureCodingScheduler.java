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
import org.smartdata.action.annotation.ActionSignature;
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
  private String ecActionID;
  private String unecActionID;
  private String checkecActionID;
  private String listecActionID;
  private List<String> actions;
  public static final String EC_DIR = "/system/ssm/ec_tmp/";
  public static final String ORIGIN_DIR = "/system/ssm/origin_tmp/";
  public static final String EC_TMP = "-ecTmp";
  public static final String ORIGIN_TMP = "-originTmp";
  private Set<String> fileLock;
  private SmartConf conf;
  private MetaStore metaStore;

  public ErasureCodingScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.conf = context.getConf();
    this.metaStore = metaStore;

    if (isECSupported()) {
      ecActionID = ErasureCodingAction.class.getAnnotation(ActionSignature.class).actionId();
      unecActionID = UnErasureCodingAction.class.getAnnotation(ActionSignature.class).displayName();
      checkecActionID = CheckErasureCodingPolicy.class.getAnnotation(ActionSignature.class).displayName();
      listecActionID = ListErasureCodingPolicy.class.getAnnotation(ActionSignature.class).displayName();
      actions = Arrays.asList(ecActionID, unecActionID, checkecActionID, listecActionID);
    }
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

  public static boolean isECSupported () {
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

    if (fileLock.contains(srcPath)) {
      return ScheduleResult.FAIL;
    }
    try {
      if (!metaStore.getFile(srcPath).isdir()) {
        // For ec or unec, add tmp argument
        String tmpName = createTmpName(action);
        action.getArgs().put(EC_TMP, EC_DIR + tmpName);
        actionInfo.getArgs().put(ORIGIN_TMP, ORIGIN_DIR + tmpName);
      }
    } catch (MetaStoreException ex) {
      LOG.error("Error occurred for getting file info", ex);
    }
    fileLock.add(srcPath);
    return ScheduleResult.SUCCESS;
  }

  private String createTmpName(LaunchAction action) {
    // need update to DB
    String path = action.getArgs().get(HdfsAction.FILE_PATH);
    String fileName;
    int index = path.lastIndexOf("/");
    if (index == path.length() - 1) {
      index = path.substring(0, path.length() - 1).indexOf("/");
      fileName = path.substring(index + 1, path.length() - 1);
    } else {
      fileName = path.substring(index + 1, path.length());
    }
    // The dest tmp file is under EC_DIR and
    // named by fileName, aidxxx and current time in millisecond with "_" separated
    String tmpName = fileName + "_" + "aid" + action.getActionId() +
        "_" + System.currentTimeMillis();
    return tmpName;
  }

  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.getActionName().equals(ecActionID) ||
        actionInfo.getActionName().equals(unecActionID)) {
      fileLock.remove(actionInfo.getArgs().get(HdfsAction.FILE_PATH));
    }
  }
}
