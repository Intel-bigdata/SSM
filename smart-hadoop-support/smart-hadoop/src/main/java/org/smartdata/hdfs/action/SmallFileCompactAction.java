/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs.action;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.CompatibilityHelperLoader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;

/**
 * An action to compact small files to a big container file.
 */
@ActionSignature(
    actionId = "compact",
    displayName = "compact",
    usage = HdfsAction.FILE_PATH + " $files " + SmallFileCompactAction.CONTAINER_FILE + " $container_file "
)
public class SmallFileCompactAction extends HdfsAction {
  private Configuration conf;
  public static final String CONTAINER_FILE = "-containerFile";
  private String smallFiles;
  private String containerFile;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.smallFiles = args.get(FILE_PATH);
    this.containerFile = args.get(CONTAINER_FILE);
  }

  @Override
  protected void execute() throws Exception {
    appendLog(String.format("Action starts at %s : small_file_compact %s",
        Utils.getFormatedCurrentTime(), containerFile));

    ArrayList<String> fileList = new Gson().fromJson(smallFiles, new ArrayList<String>().getClass());
    OutputStream out = getOutputStream(containerFile);
    for (String smallFile : fileList) {
      Long fileLen = getFileLength(smallFile);
      if (fileLen > 0) {
        appendLog(String.format("Compacting %s to %s", smallFile, containerFile));
        compact(smallFile, out, fileLen);
      }
    }
    if (out != null) {
      out.close();
    }
    appendLog(String.format("Compact small files to %s successfully", containerFile));
  }

  /**
   * Compact the small file to the big container file.
   */
  private void compact(String path, OutputStream out, long fileLen) throws IOException {
    InputStream in = null;
    try {
      in = getInputStream(path);
      byte[] buf = new byte[4096];
      int bytesRemaining = (int) fileLen;
      while (bytesRemaining > 0L) {
        int bytesToRead = (bytesRemaining < buf.length) ? bytesRemaining : buf.length;
        int bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1) {
          break;
        }
        out.write(buf, 0, bytesRead);
        bytesRemaining -= (long) bytesRead;
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  /**
   * Get output stream for the specified file.
   */
  private OutputStream getOutputStream(String path) throws IOException {
    if (path.startsWith("hdfs")) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      int replication = DFSConfigKeys.DFS_REPLICATION_DEFAULT;
      if (fs.exists(new Path(path))) {
        return fs.append(new Path(path));
      } else {
        return fs.create(new Path(path), true, (short) replication);
      }
    } else {
      if (dfsClient.exists(path)) {
        return CompatibilityHelperLoader.getHelper()
            .getDFSClientAppend(dfsClient, path, 64 * 1024, 0);
      } else {
        return dfsClient.create(path, true);
      }
    }
  }

  /**
   * Get input stream for the specified file.
   */
  private InputStream getInputStream(String path) throws IOException {
    if (path.startsWith("hdfs")) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      return fs.open(new Path(path));
    } else {
      return dfsClient.open(path);
    }
  }

  /**
   * Get length of the specified file.
   */
  private long getFileLength(String path) throws IOException {
    if (path.startsWith("hdfs")) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      return fs.getFileStatus(new Path(path)).getLen();
    } else {
      return dfsClient.getFileInfo(path).getLen();
    }
  }
}
