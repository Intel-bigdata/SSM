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

import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.CompatibilityHelperLoader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.EnumSet;
/**
 * An action to copy a single file from src to destination.
 * If dest doesn't contains "hdfs" prefix, then destination will be set to
 * current cluster, i.e., copy between dirs in current cluster.
 * Note that destination should contains filename.
 */
@ActionSignature(
    actionId = "copy2s3",
    displayName = "copy2s3",
    usage = HdfsAction.FILE_PATH + " $src " + Copy2S3Action.DEST +
        " $dest "
)
public class Copy2S3Action extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CopyFileAction.class);
  public static final String BUF_SIZE = "-bufSize";
  public static final String SRC = HdfsAction.FILE_PATH;
  public static final String DEST = "-dest";
  private String srcPath;
  private String destPath;
  private int bufferSize = 64 * 1024;
  private Configuration conf;

  @Override
  public void init(Map<String, String> args) {
    try {
      this.conf = getContext().getConf();
      String nameNodeURL =
          this.conf.get(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY);
      conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, nameNodeURL);
    } catch (NullPointerException e) {
      this.conf = new Configuration();
      appendLog("Conf error!, NameNode URL is not configured!");
    }
    super.init(args);
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(DEST)) {
      this.destPath = args.get(DEST);
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
        String.format("Action starts at %s : Read %s",
            Utils.getFormatedCurrentTime(), srcPath));
    if (!dfsClient.exists(srcPath)) {
      throw new ActionException("CopyFile Action fails, file doesn't exist!");
    }
    appendLog(
        String.format("Copy from %s to %s", srcPath, destPath));
    copySingleFile(srcPath, destPath);
    appendLog("Copy Successfully!!");
    setXAttribute(srcPath, destPath);
    appendLog("SetXattr Successfully!!");
}

  private long getFileSize(String fileName) throws IOException {
    if (fileName.startsWith("hdfs")) {
      // Get InputStream from URL
      FileSystem fs = FileSystem.get(URI.create(fileName), conf);
      return fs.getFileStatus(new Path(fileName)).getLen();
    } else {
      return dfsClient.getFileInfo(fileName).getLen();
    }
  }

  private boolean setXAttribute(String src, String dest) throws IOException {

    String name = "user.coldloc";
    dfsClient.setXAttr(srcPath, name, dest.getBytes(), EnumSet.of(XAttrSetFlag.CREATE,XAttrSetFlag.REPLACE) );
    appendLog(" SetXattr feature is set - srcPath  " + srcPath + "destination" + dest.getBytes() );
    return true; 
  }

  private boolean copySingleFile(String src, String dest) throws IOException {
    //get The file size of source file
    InputStream in = null;
    OutputStream out = null;

    try {
      in = getSrcInputStream(src);
      out = CompatibilityHelperLoader.getHelper().getS3outputStream(dest,conf);
      byte[] buf = new byte[bufferSize];
      long bytesRemaining = getFileSize(src);

      while (bytesRemaining > 0L) {
        int bytesToRead =
            (int) (bytesRemaining < (long) buf.length ? bytesRemaining :
                (long) buf.length);
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

  private InputStream getSrcInputStream(String src) throws IOException {
    if (!src.startsWith("hdfs")) {
      // Copy between different remote clusters
      // Get InputStream from URL
      FileSystem fs = FileSystem.get(URI.create(src), conf);
      return fs.open(new Path(src));
    } else {
      // Copy from primary HDFS
      return dfsClient.open(src);
    }
  }

  private OutputStream getDestOutPutStream(String dest) throws IOException {
    // Copy to remote S3
    if (!dest.startsWith("s3")) {
      throw new IOException();
    }
    // Copy to s3
    FileSystem fs = FileSystem.get(URI.create(dest), conf);
    return fs.create(new Path(dest), true);
  }
}
