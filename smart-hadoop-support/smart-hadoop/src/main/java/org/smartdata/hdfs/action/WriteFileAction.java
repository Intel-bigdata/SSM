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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConfKeys;

import java.io.OutputStream;
import java.util.Map;
import java.util.Random;

/**
 * An action to write a file with generated content. Can be used to test: 1. storage policy; 2.
 * stripping erasure coded file; 3. small file.
 *
 * <p>Arguments: file_path length [buffer_size, default=64k]
 */
@ActionSignature(
  actionId = "write",
  displayName = "write",
  usage =
      HdfsAction.FILE_PATH
          + " $file "
          + WriteFileAction.BUF_SIZE
          + " $size "
          + WriteFileAction.LENGTH
          + " $length"
)
public class WriteFileAction extends HdfsAction {
  public static final String LENGTH = "-length";
  public static final String BUF_SIZE = "-bufSize";
  private String filePath;
  private long length = -1;
  private int bufferSize = 64 * 1024;
  private Configuration conf;

  @Override
  public void init(Map<String, String> args) {
    try {
      this.conf = getContext().getConf();
      String nameNodeURL = this.conf.get(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY);
      conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, nameNodeURL);
    } catch (NullPointerException e) {
      this.conf = new Configuration();
      appendLog("Conf error!, NameNode URL is not configured!");
    }
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
    if (filePath == null) {
      throw new IllegalArgumentException("File parameter is missing! ");
    }
    if (length == -1) {
      throw new IllegalArgumentException("Write Action provides wrong length! ");
    }
    appendLog(
        String.format(
            "Action starts at %s : Write %s with length %s",
            Utils.getFormatedCurrentTime(), filePath, length));

    Path path = new Path(filePath);
    FileSystem fileSystem = path.getFileSystem(conf);
    int replication = fileSystem.getServerDefaults(new Path(filePath)).getReplication();
    final FSDataOutputStream out = fileSystem.create(path, true, replication);
    // generate random data with given length
    byte[] buffer = new byte[bufferSize];
    new Random().nextBytes(buffer);
    appendLog(String.format("Generate random data with length %d", length));
    for (long pos = 0; pos < length; pos += bufferSize) {
      long writeLength = pos + bufferSize < length ? bufferSize : length - pos;
      out.write(buffer, 0, (int) writeLength);
    }
    out.close();
    appendLog("Write Successfully!");
  }
}
