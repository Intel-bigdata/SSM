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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionType;
import org.smartdata.actions.hdfs.move.MoveRunner;
import org.smartdata.actions.hdfs.move.MoverBasedMoveRunner;
import org.smartdata.actions.hdfs.move.MoverStatus;

import java.util.Date;

/**
 * An action to set and enforce storage policy for a file.
 */
public class MoveFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(MoveFileAction.class);

  public String storagePolicy;
  private String fileName;
  private ActionType actionType;
  private MoveRunner moveRunner = null;
  private String name = "MoveFileAction";

  public MoveFileAction() {
    this.actionType = ActionType.MoveFile;
    this.setActionStatus(new MoverStatus());
  }

  public String getName() {
    return name;
  }

  @Override
  public void init(String[] args) {
    super.init(args);
    this.fileName = args[0];
    this.storagePolicy = args[1];
  }

  public MoveFileAction setMoverBasedMoveRunner() {
    moveRunner = new MoverBasedMoveRunner(
        getContext().getConf(), getActionStatus());
    return this;
  }

  public MoveFileAction setMapReduceBasedMoveRunner() {

  }
  
  /**
   * Execute an action.
   *
   * @return true if success, otherwise return false.
   */
  protected void execute() {
    // TODO check if storagePolicy is the same
    logOut.println("Action starts at "
        + (new Date(System.currentTimeMillis())).toString() + " : "
        + fileName + " -> " + storagePolicy.toString());
    try {
      dfsClient.setStoragePolicy(fileName, storagePolicy);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    MoverBasedMoveRunner moverRunner = new MoverBasedMoveRunner(
        getContext().getConf(), getActionStatus());
    try {
      moverRunner.move(fileName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
