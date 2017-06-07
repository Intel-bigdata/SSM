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
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.Random;

/**
 * An action to write a file with generated content.
 * Can be used to test:
 * 1. storage policy;
 * 2. stripping erasure coded file;
 * 3. small file.
 *
 * Arguments: file_path length [buffer_size, default=64k]
 */
public class WriteFileAction extends HdfsAction {
  private String filePath;
  private int length = -1;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(String[] args) {
    super.init(args);
    this.filePath = args[0];
    if (args.length < 2) {
      return;
    }
    this.length = Integer.valueOf(args[1]);
    if (args.length >= 3) {
      this.bufferSize = Integer.valueOf(args[2]);
    }
  }

  @Override
  protected void execute() {
    logOut.println("Action starts at "
        + (new Date(System.currentTimeMillis())).toString() + " : Write "
        + filePath + String.format(" with length %d", length));
    ActionStatus actionStatus = getActionStatus();
    actionStatus.begin();
    try {
      if (length == -1) {
        resultOut.println("Write Action provides wrong length!");
        throw new IOException();
      }
      final OutputStream out = dfsClient.create(filePath, true);
      // generate random data with given length
      byte[] buffer = new byte[bufferSize];
      new Random().nextBytes(buffer);
      logOut.println(String.format("Generate random data with length %d", length));
      // write to HDFS
      for (int pos = 0; pos < length; pos += bufferSize) {
        int writeLength = pos + bufferSize < length ? bufferSize : length - pos;
        out.write(buffer, 0, writeLength);
      }
      out.close();
      logOut.println("Write Successfully!");
      actionStatus.setSuccessful(true);
    } catch (IOException e) {
      actionStatus.setSuccessful(false);
      resultOut.println("WriteFile Action fails!\n" + e);
    } finally {
      actionStatus.end();
    }
  }
}
