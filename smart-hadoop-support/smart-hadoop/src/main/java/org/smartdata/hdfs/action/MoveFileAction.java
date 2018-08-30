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
package org.smartdata.hdfs.action;

import com.google.gson.Gson;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionType;
import org.smartdata.action.Utils;
import org.smartdata.hdfs.action.move.AbstractMoveFileAction;
import org.smartdata.hdfs.action.move.MoverExecutor;
import org.smartdata.hdfs.action.move.MoverStatus;
import org.smartdata.model.action.FileMovePlan;

import java.io.IOException;
import java.util.Map;

/**
 * An action to set and enforce storage policy for a file.
 */
public class MoveFileAction extends AbstractMoveFileAction {
  private static final Logger LOG = LoggerFactory.getLogger(MoveFileAction.class);
  private MoverStatus status;
  private String storagePolicy;
  private String fileName;
  private FileMovePlan movePlan;

  public MoveFileAction() {
    super();
    this.actionType = ActionType.MoveFile;
    this.status = new MoverStatus();
  }

  public MoverStatus getStatus() {
    return this.status;
  }

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.fileName = args.get(FILE_PATH);
    this.storagePolicy = getStoragePolicy() != null ?
        getStoragePolicy() : args.get(STORAGE_POLICY);
    if (args.containsKey(MOVE_PLAN)) {
      String plan = args.get(MOVE_PLAN);
      if (plan != null) {
        Gson gson = new Gson();
        movePlan = gson.fromJson(plan, FileMovePlan.class);
        status.setTotalBlocks(movePlan.getBlockIds().size());
      }
    }
  }

  @Override
  protected void execute() throws Exception {
    if (fileName == null) {
      throw new IllegalArgumentException("File parameter is missing!");
    }

    if (movePlan == null) {
      throw new IllegalArgumentException("File move plan not specified.");
    }

    if (movePlan.isDir()) {
      dfsClient.setStoragePolicy(fileName, storagePolicy);
      appendLog("Directory moved successfully.");
      return;
    }

    int totalReplicas = movePlan.getBlockIds().size();
    this.appendLog(
        String.format(
            "Action starts at %s : %s -> %s with %d replicas to move in total.",
            Utils.getFormatedCurrentTime(), fileName, storagePolicy, totalReplicas));

    int numFailed = move();
    if (numFailed == 0) {
      appendLog("All scheduled " + totalReplicas + " replicas moved successfully.");
      if (movePlan.isBeingWritten() || recheckModification()) {
        appendResult("UpdateStoragePolicy=false");
        appendLog("NOTE: File may be changed during executing this action. "
            + "Will move the corresponding blocks later.");
      }
    } else {
      String res = numFailed + " of " + totalReplicas + " replicas movement failed.";
      appendLog(res);
      throw new IOException(res);
    }
  }

  private int move() throws Exception {
    int maxMoves = movePlan.getPropertyValueInt(FileMovePlan.MAX_CONCURRENT_MOVES, 10);
    int maxRetries = movePlan.getPropertyValueInt(FileMovePlan.MAX_NUM_RETRIES, 10);
    MoverExecutor executor = new MoverExecutor(status, getContext().getConf(), maxRetries, maxMoves);
    return executor.executeMove(movePlan, getResultOs(), getLogOs());
  }

  private boolean recheckModification() {
    try {
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileName);
      if (fileStatus == null) {
        return true;
      }

      boolean closed = dfsClient.isFileClosed(fileName);
      if (!closed
          || (movePlan.getFileId() != 0 && fileStatus.getFileId() != movePlan.getFileId())
          || fileStatus.getLen() != movePlan.getFileLength()
          || fileStatus.getModificationTime() != movePlan.getModificationTime()) {
        return true;
      }
      return false;
    } catch (Exception e) {
      return true; // check again for this case
    }
  }

  @Override
  public float getProgress() {
    return this.status.getPercentage();
  }

  public String getStoragePolicy() {
    return storagePolicy;
  }
}
