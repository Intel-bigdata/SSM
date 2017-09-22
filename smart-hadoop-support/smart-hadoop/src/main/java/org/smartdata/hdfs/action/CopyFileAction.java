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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;

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
    usage = HdfsAction.FILE_PATH + " $src " + CopyFileAction.DEST_PATH +
        " $dest "  + CopyFileAction.OFFSET_INDEX + " $offset" + CopyFileAction.LENGTH +
        " $length" + CopyFileAction.BUF_SIZE + " $size"
)
public class CopyFileAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CopyFileAction.class);
  public static final String BUF_SIZE = "-bufSize";
  public static final String DEST_PATH = "-dest";
  public static final String OFFSET_INDEX = "-offset";
  public static final String LENGTH = "-length";
  private String srcPath;
  private String destPath;
  private long offset = 0;
  private long length = 0;
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
    if (args.containsKey(OFFSET_INDEX)) {
      offset = Integer.valueOf(args.get(OFFSET_INDEX));
    }
    if (args.containsKey(LENGTH)) {
      length = Integer.valueOf(args.get(LENGTH));
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
        String.format("Action starts at %s : Read %s",
            Utils.getFormatedCurrentTime(), srcPath));
    if (!dfsClient.exists(srcPath)) {
      throw new ActionException("CopyFile Action fails, file doesn't exist!");
    }
    appendLog(
        String.format("Copy from %s to %s", srcPath, destPath));
    if (offset == 0 && length == 0) {
      copySingleFile(srcPath, destPath);
    }
    if (length != 0) {
      copyWithOffset(srcPath, destPath, bufferSize, offset, length);
    }
  }

  private boolean copySingleFile(String src, String dest) throws IOException {
    //get The file size of source file
    long fileSize = getFileSize(src);
    return copyWithOffset(src,dest,2048,0,fileSize);
  }

  private boolean copyWithOffset(String src, String dest, int bufferSize, long offset, long length) throws IOException {
    InputStream in = null;
    OutputStream out = null;

    try {
      in = getSrcInputStream(src);
      out = getDestOutPutStream(dest);

      //skip offset
      in.skip(offset);

      byte[] buf = new byte[bufferSize];
      long bytesRemaining = length;

      while (bytesRemaining > 0L) {
        int bytesToRead = (int) (bytesRemaining < (long) buf.length ? bytesRemaining : (long) buf.length);
        int bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1) {
          break;
        }
        out.write(buf, 0, bytesRead);
        bytesRemaining -= (long) bytesRead;
      }

      return true;
    } finally {
      if (out != null) {
        out.close();
      }
      if (in != null) {
        in.close();
      }
    }
  }

  private long getFileSize(String fileName) throws IOException {
    if (fileName.startsWith("hdfs")) {
      Configuration conf = new Configuration();
      // Get InputStream from URL
      FileSystem fs = FileSystem.get(URI.create(fileName), conf);
      return fs.getFileStatus(new Path(fileName)).getLen();
    } else {
      return dfsClient.getFileInfo(fileName).getLen();
    }
  }

  private InputStream getSrcInputStream(String src) throws IOException {
    if (src.startsWith("hdfs")) {
      // Copy between different remote clusters
      // TODO read conf from files
      Configuration conf = new Configuration();
      // Get InputStream from URL
      FileSystem fs = FileSystem.get(URI.create(src), conf);
      return fs.open(new Path(src));
    } else {
      return dfsClient.open(src);
    }
  }

  private OutputStream getDestOutPutStream(String dest) throws IOException {
    if (dest.startsWith("hdfs")) {
      // Copy between different clusters
      // TODO read conf from files
      Configuration conf = new Configuration();
      // Get OutPutStream from URL
      FileSystem fs = FileSystem.get(URI.create(dest), conf);
      try {
        int replication = fs.getServerDefaults(new Path(dest)).getReplication();
        if (replication != DFSConfigKeys.DFS_REPLICATION_DEFAULT) {
          LOG.debug("Remote Replications =" + replication);
          conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replication);
          fs = FileSystem.get(URI.create(dest), conf);
        }
      } catch (IOException e) {
        LOG.debug("Get Server default replication error!", e);
      }
      if (fs.exists(new Path(dest))) {
        return fs.append(new Path(dest));
      }
      return fs.create(new Path(dest), true);
    } else {
      // Copy between different dirs of the same cluster
      if (dfsClient.exists(dest)) {
        // TODO local append
      }
      return dfsClient.create(dest, true);
    }
  }
}
