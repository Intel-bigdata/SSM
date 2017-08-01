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
package org.smartdata.actions.hdfs;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionType;
import org.smartdata.actions.Utils;
import org.smartdata.actions.hdfs.move.MoveRunner;
import org.smartdata.actions.hdfs.move.MoverBasedMoveRunner;
import org.smartdata.actions.hdfs.move.MoverStatus;
import org.smartdata.actions.hdfs.move.OldMoveRunner;
import org.smartdata.actions.hdfs.move.OldMoverBasedMoveRunner;
import org.smartdata.model.actions.hdfs.SchedulePlan;

import java.util.Map;

/**
 * An action to set and enforce storage policy for a file.
 */
public class MoveFileAction extends HdfsAction {
  public static final String STORAGE_POLICY = "-storagePolicy";
  public static final String MOVE_PLAN = "-movePlan";
  private static final Logger LOG = LoggerFactory.getLogger(MoveFileAction.class);
  private MoverStatus status;
  private String storagePolicy;
  private String fileName;
  private SchedulePlan movePlan;

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
        movePlan = gson.fromJson(plan, SchedulePlan.class);
      }
    }
  }

  @Override
  protected void execute() throws Exception {
    if (fileName == null) {
      throw new IllegalArgumentException("File parameter is missing! ");
    }
    this.appendLog(
        String.format(
            "Action starts at %s : %s -> %s",
            Utils.getFormatedCurrentTime(), fileName, storagePolicy));
    dfsClient.setStoragePolicy(fileName, storagePolicy);

    // TODO : make MoveRunner configurable
    if (movePlan == null) {
      OldMoveRunner moveRunner = new OldMoverBasedMoveRunner(getContext().getConf(), this.status);
      moveRunner.move(fileName);
    } else {
      MoveRunner moveRunner = new MoverBasedMoveRunner(getContext().getConf(), this.status);
      moveRunner.move(fileName, movePlan);
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
