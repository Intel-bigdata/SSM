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

import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.util.Date;

/**
 * An action to read a file. The read content will be discarded immediately, not storing onto disk.
 * Can be used to test:
 * 1. cache file;
 * 2. one-ssd/all-ssd file;
 *
 * Arguments: file_path [buffer_size, default=64k]
 */
public class ReadFileAction extends HdfsAction {

  private String filePath;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(String[] args) {
    super.init(args);
    this.filePath = args[0];
    if (args.length >= 2) {
      bufferSize = Integer.valueOf(args[1]);
    }
  }

  @Override
  protected void execute() {
    logOut.println("Action starts at "
        + (new Date(System.currentTimeMillis())).toString() + " : Read "
        + filePath);
    ActionStatus actionStatus = getActionStatus();
    actionStatus.begin();
    try {
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(filePath);
      if (fileStatus == null) {
        resultOut.println("ReadFile Action fails, file doesn't exist!");
      }
      DFSInputStream dfsInputStream = dfsClient.open(filePath);
      byte[] buffer = new byte[bufferSize];
      // read from HDFS
      while (dfsInputStream.read(buffer, 0, bufferSize) != -1) {
      }
      dfsInputStream.close();
      actionStatus.setSuccessful(true);
    } catch (IOException e) {
      actionStatus.setSuccessful(false);
      resultOut.println("ReadFile Action fails!\n" + e.getMessage());
    } finally {
      actionStatus.end();
    }
  }
}
