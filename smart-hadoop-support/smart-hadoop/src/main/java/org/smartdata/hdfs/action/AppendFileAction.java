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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.smartdata.action.ActionException;
import org.smartdata.action.annotation.ActionSignature;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

@ActionSignature(
  actionId = "append",
  displayName = "append",
  usage = HdfsAction.FILE_PATH + " $src" +
      AppendFileAction.LENGTH + " $length" +
      AppendFileAction.BUF_SIZE + " $size"
)
public class AppendFileAction extends HdfsAction {
  static final String BUF_SIZE = "-bufSize";
  static final String LENGTH = "-length";
  private String srcPath;
  private long length = 1024;
  private int bufferSize = 64 * 1024;
  private Configuration conf;

  @Override
  public void init(Map<String, String> args) {
    try {
      this.conf = getContext().getConf();
    } catch (NullPointerException e) {
      this.conf = new Configuration();
    }
    super.init(args);
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE)) {
      bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
    if (args.containsKey(LENGTH)) {
      length = Long.valueOf(args.get(LENGTH));
    }
  }

  @VisibleForTesting
  protected void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath != null && !srcPath.isEmpty()) {
      Path path = new Path(srcPath);
      FileSystem fileSystem = path.getFileSystem(conf);
      if (!fileSystem.exists(path)) {
        throw new ActionException("Append Action fails, file doesn't exist!");
      }
      Random random = new Random();
      FSDataOutputStream os = null;
      try {
        os = fileSystem.append(path, bufferSize);
        long remaining = length;
        while (remaining > 0) {
          int toAppend = (int) Math.min(remaining, bufferSize);
          byte[] bytes = new byte[toAppend];
          random.nextBytes(bytes);
          os.write(bytes);
          remaining -= toAppend;
        }
      } finally {
        if (os != null) {
          os.close();
        }
      }
    } else {
      throw new ActionException("File parameter is missing.");
    }
  }
}
