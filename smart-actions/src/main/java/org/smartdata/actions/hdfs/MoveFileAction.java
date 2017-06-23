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
import org.smartdata.actions.ActionException;
import org.smartdata.actions.ActionType;
import org.smartdata.actions.hdfs.move.MoveRunner;
import org.smartdata.actions.hdfs.move.MoverBasedMoveRunner;
import org.smartdata.actions.hdfs.move.MoverStatus;

import java.util.Date;
import java.util.Map;

/**
 * An action to set and enforce storage policy for a file.
 */
public class MoveFileAction extends HdfsAction {
  public static final String STORAGE_POLICY = "-storagePolicy";
  private static final Logger LOG = LoggerFactory.getLogger(MoveFileAction.class);

  protected String storagePolicy;
  protected String fileName;
  protected ActionType actionType;
  protected MoveRunner moveRunner = null;

  public MoveFileAction() {
    this.actionType = ActionType.MoveFile;
    createStatus();
  }

  @Override
  protected void createStatus() {
    this.actionStatus = new MoverStatus();
    resultOut = actionStatus.getResultPrintStream();
    logOut = actionStatus.getLogPrintStream();
  }

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.fileName = args.get(FILE_PATH);
    this.storagePolicy = args.get(STORAGE_POLICY);
  }

  @Override
  protected void execute() throws ActionException {
    logOut.println("Action starts at "
        + (new Date(System.currentTimeMillis())).toString() + " : "
        + fileName + " -> " + storagePolicy.toString());
    try {
      dfsClient.setStoragePolicy(fileName, storagePolicy);
    } catch (Exception e) {
      throw new ActionException(e);
    }

    // TODO : make MoveRunner configurable
    moveRunner = new MoverBasedMoveRunner(getContext().getConf(), getActionStatus());

    try {
      moveRunner.move(fileName);
    } catch (Exception e) {
      throw new ActionException(e);
    }
  }
}
