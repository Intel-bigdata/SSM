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

//import org.smartdata.hdfs.action.ErasureCodingAction;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.util.*;

public class ErasureCodingScheduler extends ActionSchedulerService {
  public static final Logger LOG = LoggerFactory.getLogger(ErasureCodingScheduler.class);
  private static final List<String> ACTIONS = Arrays.asList("ec", "unec", "checkec");
  // The arguments which can be set by user
  public static Set<String> arguments = new HashSet<>();
  public static final String EC_DIR = "/system/ssm/ec_tmp";
  private Set<String> fileLock;
  private SmartConf conf;
  private MetaStore metaStore;

  public ErasureCodingScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.conf = context.getConf();
    this.metaStore = metaStore;
  }
  public List<String> getSupportedActions() {
    return ACTIONS;
  }

  public void init() throws IOException {
    fileLock = new HashSet<>();
//    arguments.addAll(Arrays.asList(HdfsAction.FILE_PATH, ErasureCodingAction.EC_POLICY_NAME,
//        ErasureCodingAction.BUF_SIZE));
  }

  public void start() throws IOException {

  }

  public void stop() throws IOException {

  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) throws IOException {
    if (actionInfo.getArgs().get(HdfsAction.FILE_PATH) == null) {
      throw new IOException("No src path is given!");
    }
    String[] parts = VersionInfo.getVersion().split("\\.");
    if (Integer.parseInt(parts[0]) == 2) {
      throw new IOException(actionInfo.getActionName() +
          " is not supported on " + VersionInfo.getVersion());
    }
//    if (!arguments.containsAll(actionInfo.getArgs().keySet())) {
//      LOG.warn("Invalid arguments:" +
//          actionInfo.getArgs().keySet().removeAll(arguments));
//    }
    return true;
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (!ACTIONS.contains(action.getActionType())) {
      return ScheduleResult.SUCCESS;
    }
    String srcPath = action.getArgs().get(HdfsAction.FILE_PATH);
    if (srcPath == null) {
      actionInfo.appendLog("No file is given in this action!");
      return ScheduleResult.FAIL;
    }
    if (fileLock.contains(srcPath)) {
      return ScheduleResult.FAIL;
    }
    try {
      if (!metaStore.getFile(srcPath).isdir()) {
        actionInfo.getArgs().put("-dest", createDest(actionInfo));
      }
    } catch (MetaStoreException ex) {
      LOG.error("Error occurred for getting file info", ex);
    }

    fileLock.add(srcPath);
    return ScheduleResult.SUCCESS;
  }

  private String createDest(ActionInfo actionInfo) {
    // need update to DB
    String path = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
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
    String dest = EC_DIR + "/" + fileName + "_" + "aid" + actionInfo.getActionId() +
        "_" + System.currentTimeMillis();
    return dest;
  }

  public void postSchedule(ActionInfo actionInfo, ScheduleResult result) {
  }

  public void onPreDispatch(LaunchAction action) {
  }

  public void onActionFinished(ActionInfo actionInfo) {
    fileLock.remove(actionInfo.getArgs().get(HdfsAction.FILE_PATH));
  }
}
