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

import org.smartdata.actions.ActionException;
import org.smartdata.actions.Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
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
  public static final String LENGTH = "-length";
  public static final String BUF_SIZE = "-bufSize";
  private String filePath;
  private long length = -1;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.filePath = args.get(FILE_PATH);
    if (args.containsKey(LENGTH)) {
      length = Long.valueOf(args.get(LENGTH));
    }
    if (args.containsKey(BUF_SIZE)) {
      this.bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
  }

  @Override
  protected void execute() throws Exception {
    this.appendLog(
        String.format(
            "Action starts at %s : Write %s with length %s",
            Utils.getFormatedCurrentTime(), filePath, length));
    try {
      if (length == -1) {
        this.appendResult("Write Action provides wrong length!");
        throw new IOException();
      }
      final OutputStream out = dfsClient.create(filePath, true);
      // generate random data with given length
      byte[] buffer = new byte[bufferSize];
      new Random().nextBytes(buffer);
      this.appendLog(String.format("Generate random data with length %d", length));
      // write to HDFS
      for (int pos = 0; pos < length; pos += bufferSize) {
        long writeLength = pos + bufferSize < length ? bufferSize : length - pos;
        out.write(buffer, 0, (int)writeLength);
      }
      out.close();
      this.appendLog("Write Successfully!");
    } catch (IOException e) {
      this.appendResult("WriteFile Action fails!\n" + e);
      throw new ActionException(e);
    }
  }
}
