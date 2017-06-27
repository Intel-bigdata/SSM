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
import org.smartdata.actions.ActionException;
import org.smartdata.actions.Utils;

import java.io.IOException;
import java.util.Map;

/**
 * An action to read a file. The read content will be discarded immediately, not storing onto disk.
 * Can be used to test:
 * 1. cache file;
 * 2. one-ssd/all-ssd file;
 *
 * Arguments: file_path [buffer_size, default=64k]
 */
public class ReadFileAction extends HdfsAction {
  public static final String BUF_SIZE = "-bufSize";
  private String filePath;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.filePath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE)) {
      bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
  }

  @Override
  protected void execute() throws Exception {
    this.appendLog(
        String.format("Action starts at %s : Read %s", Utils.getFormatedCurrentTime(), filePath));
    try {
      HdfsFileStatus fileStatus = dfsClient.getFileInfo(filePath);
      if (fileStatus == null) {
        this.appendResult("ReadFile Action fails, file doesn't exist!");
      }
      DFSInputStream dfsInputStream = dfsClient.open(filePath);
      byte[] buffer = new byte[bufferSize];
      // read from HDFS
      while (dfsInputStream.read(buffer, 0, bufferSize) != -1) {
      }
      dfsInputStream.close();
    } catch (IOException e) {
      this.appendResult("ReadFile Action fails!\n" + e.getMessage());
      throw new ActionException(e);
    }
  }
}
