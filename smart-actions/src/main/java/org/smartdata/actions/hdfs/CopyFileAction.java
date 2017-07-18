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
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionException;
import org.smartdata.actions.Utils;
import org.smartdata.actions.annotation.ActionSignature;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * An action to copy a file.
 */
@ActionSignature(
    actionId = "copy",
    displayName = "copy",
    usage = HdfsAction.FILE_PATH + " $src " + CopyFileAction.REMOTE_URL + " $dist " + CopyFileAction.BUF_SIZE + " $size"
)
public class CopyFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(CopyFileAction.class);
  public static final String BUF_SIZE = "-bufSize";
  public static final String REMOTE_URL = "-backup";
  private String srcPath;
  private String distPath;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(REMOTE_URL)) {
      this.distPath = args.get(REMOTE_URL);
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
    if (distPath == null) {
      throw new IllegalArgumentException("Dist File parameter is missing.");
    }
    appendLog(
        String.format("Action starts at %s : Read %s", Utils.getFormatedCurrentTime(), srcPath));
    if (!dfsClient.exists(srcPath)) {
      throw new ActionException("CopyFile Action fails, file doesn't exist!");
    }
    InputStream srcInputStream = null;
    OutputStream distOutStream = null;
    try {
       srcInputStream = dfsClient.open(srcPath);
       distOutStream = dfsClient.create(distPath, true);
      // Copy from src to dist
      IOUtils.copyBytes(srcInputStream, distOutStream, bufferSize, false);
    } finally {
      IOUtils.closeStream(srcInputStream);
      IOUtils.closeStream(distOutStream);
    }
  }
}
