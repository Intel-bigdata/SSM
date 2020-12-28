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

import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.CompatibilityHelper;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.*;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.FileInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.LaunchCmdlet;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.smartdata.model.ActionInfo.OLD_FILE_ID;

public class ErasureCodingScheduler extends ActionSchedulerService {
  public static final Logger LOG = LoggerFactory.getLogger(ErasureCodingScheduler.class);
  public static final String EC_ACTION_ID = "ec";
  public static final String UNEC_ACTION_ID = "unec";
  public static final String CHECK_EC_ACTION_ID = "checkec";
  public static final String LIST_EC_ACTION_ID = "listec";
  public static final List<String> actions =
      Arrays.asList(EC_ACTION_ID, UNEC_ACTION_ID, CHECK_EC_ACTION_ID, LIST_EC_ACTION_ID);

  public static String EC_DIR;
  public static final String EC_TMP_DIR = "ec_tmp/";
  public static final String EC_TMP = "-ecTmp";
  public static final String EC_POLICY = "-policy";
  private Set<String> fileLock;
  private SmartConf conf;
  private MetaStore metaStore;
  private long throttleInMb;
  private RateLimiter rateLimiter;
  private DFSClient dfsClient;

  public ErasureCodingScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.conf = context.getConf();
    this.metaStore = metaStore;
    this.throttleInMb = conf.getLong(SmartConfKeys.SMART_ACTION_EC_THROTTLE_MB_KEY,
        SmartConfKeys.SMART_ACTION_EC_THROTTLE_MB_DEFAULT);
    if (this.throttleInMb > 0) {
      this.rateLimiter = RateLimiter.create(throttleInMb);
    }
    String ssmTmpDir = conf.get(
        SmartConfKeys.SMART_WORK_DIR_KEY, SmartConfKeys.SMART_WORK_DIR_DEFAULT);
    ssmTmpDir = ssmTmpDir + (ssmTmpDir.endsWith("/") ? "" : "/");
    ErasureCodingScheduler.EC_DIR = ssmTmpDir + EC_TMP_DIR;
    fileLock = new HashSet<>();
  }

  public List<String> getSupportedActions() {
    return actions;
  }

  public void init() throws IOException {
    fileLock.clear();
    try {
      final URI nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
      dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
    } catch (IOException e) {
      LOG.warn("Failed to create dfsClient.");
    }
  }

  @Override
  public void start() throws IOException {

  }

  @Override
  public void stop() throws IOException {

  }

  @Override
  public boolean onSubmit(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex)
      throws IOException {
    if (!isECSupported()) {
      throw new IOException(actionInfo.getActionName() +
          " is not supported on " + VersionInfo.getVersion());
    }
    if (actionInfo.getActionName().equals(LIST_EC_ACTION_ID)) {
      return true;
    }

    if (actionInfo.getArgs().get(HdfsAction.FILE_PATH) == null) {
      throw new IOException("File path is required for action " + actionInfo.getActionName() + "!");
    }
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    // The root dir should be excluded in checking whether file path ends with slash.
    if (!srcPath.equals("/") && srcPath.endsWith("/")) {
      srcPath = srcPath.substring(0, srcPath.length() - 1);
      actionInfo.getArgs().put(HdfsAction.FILE_PATH, srcPath);
    }
    // For ec or unec action, check if the file is locked.
    if (actionInfo.getActionName().equals(EC_ACTION_ID) ||
        actionInfo.getActionName().equals(UNEC_ACTION_ID)) {
      if (fileLock.contains(srcPath)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isECSupported() {
    String[] parts = VersionInfo.getVersion().split("\\.");
    return Integer.parseInt(parts[0]) == 3;
  }

  @Override
  public ScheduleResult onSchedule(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      LaunchCmdlet cmdlet, LaunchAction action, int actionIndex) {
    if (!actions.contains(action.getActionType())) {
      return ScheduleResult.SUCCESS;
    }

    if (actionInfo.getActionName().equals(LIST_EC_ACTION_ID)) {
      return ScheduleResult.SUCCESS;
    }

    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    if (srcPath == null) {
      actionInfo.appendLog("No file is given in this action!");
      return ScheduleResult.FAIL;
    }

    if (actionInfo.getActionName().equals(CHECK_EC_ACTION_ID)) {
      return ScheduleResult.SUCCESS;
    }

    try {
      // use the default EC policy if an ec action has not been given an EC policy
      if (actionInfo.getActionName().equals(EC_ACTION_ID)) {
        String ecPolicy = actionInfo.getArgs().get(EC_POLICY);
        if (ecPolicy == null || ecPolicy.isEmpty()) {
          String defaultEcPolicy = conf.getTrimmed("dfs.namenode.ec.system.default.policy",
              "RS-6-3-1024k");
          actionInfo.getArgs().put(EC_POLICY, defaultEcPolicy);
          action.getArgs().put(EC_POLICY, defaultEcPolicy);
        }
      }

      FileInfo fileinfo = metaStore.getFile(srcPath);
      if (fileinfo != null && fileinfo.isdir()) {
        return ScheduleResult.SUCCESS;
      }

      // The below code is just for ec or unec action with file as argument, not directory
      if (isLimitedByThrottle(srcPath)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to schedule {} due to the limitation of throttle!", actionInfo);
        }
        return ScheduleResult.RETRY;
      }
      // For ec or unec, add ecTmp argument
      String tmpName = createTmpName(action);
      action.getArgs().put(EC_TMP, EC_DIR + tmpName);
      actionInfo.getArgs().put(EC_TMP, EC_DIR + tmpName);
    } catch (MetaStoreException ex) {
      LOG.error("Error occurred for getting file info", ex);
      actionInfo.appendLog(ex.getMessage());
      return ScheduleResult.FAIL;
    }
    afterSchedule(actionInfo);
    return ScheduleResult.SUCCESS;
  }

  @Override
  public boolean isSuccessfulBySpeculation(ActionInfo actionInfo) {
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    try {
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(srcPath);
      CompatibilityHelper compatibilityHelper =
          CompatibilityHelperLoader.getHelper();
      // For unec, if current policy ID is 0, which means replication, we
      // speculate that action was executed successful.
      if (actionInfo.getActionName().equals(UNEC_ACTION_ID)) {
        return
            compatibilityHelper.getErasureCodingPolicy(fileStatus) == (byte) 0;
      } else if (actionInfo.getActionName().equals(EC_ACTION_ID)) {
        String currentSrcEcPolicyName =
            compatibilityHelper.getErasureCodingPolicyName(fileStatus);
        String actionEcPolicyName = actionInfo.getArgs().get(EC_POLICY);
        return currentSrcEcPolicyName.equals(actionEcPolicyName);
      }
      return false;
    } catch (IOException e) {
      LOG.warn("Failed to get file status or EC policy, suppose this action " +
          "was not successfully executed: {}", actionInfo.toString());
      return false;
    }
  }

  /**
   * For EC/UnEC action, the src file will be locked and
   * the old file id is kept in a map.
   */
  public void afterSchedule(ActionInfo actionInfo) {
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    // lock the file only if ec or unec action is scheduled
    fileLock.add(srcPath);
    try {
      setOldFileId(actionInfo);
    } catch (Throwable t) {
      // We think it may not be a big issue, so just warn user this issue.
      LOG.warn("Failed in maintaining old fid for taking over old data's temperature.");
    }
  }

  /**
   * Set old file id which will be persisted into DB. For action status
   * recovery case, the old file id can be acquired for taking over old file's
   * data temperature.
   */
  private void setOldFileId(ActionInfo actionInfo) throws IOException {
    if (actionInfo.getArgs().get(OLD_FILE_ID) != null &&
        !actionInfo.getArgs().get(OLD_FILE_ID).isEmpty()) {
      return;
    }
    List<Long> oids = new ArrayList<>();
    String path = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    try {
      oids.add(dfsClient.getFileInfo(path).getFileId());
    } catch (IOException e) {
      LOG.warn("Failed to set old fid for taking over data temperature!");
      throw e;
    }
    actionInfo.setOldFileIds(oids);
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
  public void onActionFinished(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
    if (!actionInfo.isFinished()) {
      return;
    }
    if (actionInfo.getActionName().equals(EC_ACTION_ID) ||
        actionInfo.getActionName().equals(UNEC_ACTION_ID)) {
      String filePath = null;
      try {
        filePath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
        if (!actionInfo.isSuccessful()) {
          return;
        }
        // Task over access count after successful execution.
        takeOverAccessCount(actionInfo);
      } finally {
        // As long as the action is finished, regardless of success or not,
        // we should remove the corresponding record from fileLock.
        if (filePath != null) {
          fileLock.remove(filePath);
        }
      }
    }
  }

  /**
   * In rename case, the fid of renamed file is not changed. But sometimes, we need
   * to keep old file's access count and let new file takes over this metric. E.g.,
   * with (un)EC/(de)Compress/(un)Compact action, a new file will overwrite the old file.
   */
  public void takeOverAccessCount(ActionInfo actionInfo) {
    try {
      String filePath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      assert actionInfo.getOldFileIds().size() == 1;
      long oldFid = actionInfo.getOldFileIds().get(0);
      // The new fid may have not been updated in metastore, so
      // we get it from dfs client.
      long newFid = dfsClient.getFileInfo(filePath).getFileId();
      metaStore.updateAccessCountTableFid(oldFid, newFid);
    } catch (Exception e) {
      LOG.warn("Failed to take over file access count, which can make the " +
          "measure for data temperature inaccurate!", e);
    }
  }

  public boolean isLimitedByThrottle(String srcPath) throws MetaStoreException {
    if (this.rateLimiter == null) {
      return false;
    }
    int fileLengthInMb = (int) metaStore.getFile(srcPath).getLength() >> 20;
    if (fileLengthInMb > 0) {
      return !rateLimiter.tryAcquire(fileLengthInMb);
    }
    return false;
  }
}
