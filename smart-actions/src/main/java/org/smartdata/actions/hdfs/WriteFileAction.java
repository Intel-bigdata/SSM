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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;

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
  private static final Logger LOG = LoggerFactory.getLogger(WriteFileAction.class);
  private String filePath;
  private int length;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(String[] args) {
    this.filePath = args[0];
    this.length = Integer.valueOf(args[1]);
    if (args.length >= 3) {
      bufferSize = Integer.valueOf(args[2]);
    }
  }

  @Override
  protected UUID execute() {
    try {
      final OutputStream out = dfsClient.create(filePath,true);
      // generate random data with given length
      byte[] buffer = new byte[bufferSize];
      new Random().nextBytes(buffer);
      // write to HDFS
      for (int pos = 0; pos < length; pos += bufferSize) {
        int writeLength = pos + bufferSize < length ? bufferSize : length - pos;
        out.write(buffer, 0, writeLength);
      }
      out.close();
    } catch (IOException e) {
      resultOut.println("WriteFile Action fails!\n" + e.getMessage());
    }
    return null;
  }
}
