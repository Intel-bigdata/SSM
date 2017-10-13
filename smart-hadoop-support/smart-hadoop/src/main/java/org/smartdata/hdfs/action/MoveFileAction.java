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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionType;
import org.smartdata.action.Utils;
import org.smartdata.hdfs.action.move.MoverBasedMoveRunner;
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
      appendResult("Directory moved successfully.");
      return;
    }

    int totalReplicas = movePlan.getBlockIds().size();
    this.appendLog(
        String.format(
            "Action starts at %s : %s -> %s with %d replicas to move in total.",
            Utils.getFormatedCurrentTime(), fileName, storagePolicy, totalReplicas));

    MoverBasedMoveRunner moveRunner =
        new MoverBasedMoveRunner(getContext().getConf(), this.status, getResultOs(), getLogOs());
    int numFailed = moveRunner.move(fileName, movePlan);
    if (numFailed == 0) {
      dfsClient.setStoragePolicy(fileName, storagePolicy);
      appendResult("All the " + totalReplicas + " replicas moved successfully.");
    } else {
      String res = numFailed + " of " + totalReplicas + " replicas movement failed.";
      appendResult(res);
      throw new IOException(res);
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
