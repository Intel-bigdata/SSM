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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionException;
import org.smartdata.actions.Utils;
import org.smartdata.actions.annotation.ActionSignature;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

/**
 * An action to copy a single file from src to destination.
 * If dest doesn't contains "hdfs" prefix, then destination will be set to
 * current cluster, i.e., copy between dirs in current cluster.
 * Note that destination should contains filename.
 */
@ActionSignature(
    actionId = "copy",
    displayName = "copy",
    usage = HdfsAction.FILE_PATH + " $src " + CopyFileAction.DEST_PATH + " $dest " + CopyFileAction.BUF_SIZE + " $size"
)
public class CopyFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(CopyFileAction.class);
  public static final String BUF_SIZE = "-bufSize";
  public static final String DEST_PATH = "-dest";
  private String srcPath;
  private String destPath;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(DEST_PATH)) {
      this.destPath = args.get(DEST_PATH);
    }
    if (args.containsKey(BUF_SIZE)) {
      bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    if (destPath == null) {
      throw new IllegalArgumentException("Dest File parameter is missing.");
    }
    appendLog(
        String.format("Action starts at %s : Read %s", Utils.getFormatedCurrentTime(), srcPath));
    if (!dfsClient.exists(srcPath)) {
      throw new ActionException("CopyFile Action fails, file doesn't exist!");
    }
    appendLog(
        String.format("Copy from %s to %s", srcPath, destPath));
    copySingleFile(srcPath, destPath);
  }

  private boolean copySingleFile(String src, String dest) throws IOException {
    InputStream srcInputStream = null;
    OutputStream destOutStream = null;
    try {
      srcInputStream = dfsClient.open(src);
      destOutStream = getDestOutPutStream(dest);
      // Copy from src to dest
      IOUtils.copyBytes(srcInputStream, destOutStream, bufferSize, false);
      return true;
    } finally {
      IOUtils.closeStream(srcInputStream);
      IOUtils.closeStream(destOutStream);
    }
  }

  private OutputStream getDestOutPutStream(String dest) throws IOException {
    if (dest.startsWith("hdfs")) {
      // Copy between different clusters
      // TODO read conf from files
      Configuration conf = new Configuration();
      // Get OutPutStream from URL
      FileSystem fs = FileSystem.get(URI.create(dest), conf);
      return fs.create(new Path(dest));
    } else {
      // Copy between different dirs of the same cluster
      return dfsClient.create(dest, true);
    }
  }
}
