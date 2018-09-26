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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * An action to list files in a directory.
 */
@ActionSignature(
    actionId = "list",
    displayName = "list",
    usage = HdfsAction.FILE_PATH + " $src1" + ListFileAction.RECURSIVELY + " $src2" + ListFileAction.DUMP + " $src3"
        + ListFileAction.HUMAN + " $src4"
)
public class ListFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(ListFileAction.class);
  private String srcPath;
  private boolean recursively = false;
  private boolean dump = false;
  private boolean human = false;

  // Options
  public static final String RECURSIVELY = "-R";
  public static final String DUMP = "-d";
  public static final String HUMAN = "-h";

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    if (args.containsKey(RECURSIVELY)) {
      this.recursively = true;
      if (this.srcPath == null || this.srcPath == "")
        this.srcPath = args.get(RECURSIVELY);
    }
    if (args.containsKey(DUMP)) {
      this.dump = true;
      if (this.srcPath == null || this.srcPath == "")
        this.srcPath = args.get(DUMP);
    }
    if (args.containsKey(HUMAN)) {
      this.human = true;
      if (this.srcPath == null || this.srcPath == "")
        this.srcPath = args.get(HUMAN);
    }
    if (this.srcPath == null || this.srcPath == "")
      this.srcPath = args.get(FILE_PATH);
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    appendLog(
        String.format("Action starts at %s : List %s", Utils.getFormatedCurrentTime(), srcPath));
    //list the file in directionary
    listDirectory(srcPath);
  }

  private static String readableFileSize(long size) {
    if(size <= 0)
      return "0";
    final String[] units = new String[] { "", "K", "M", "G", "T" };
    int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
    if (digitGroups == 0) {
      return Long.toString(size);
    }
    return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
  }

  private void listDirectory(String src) throws IOException {
    if (!src.startsWith("hdfs")) {
      //list file in local Dir
      HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(src);
      if (hdfsFileStatus == null) {
        appendLog("File not found!");
        return;
      }
      if (hdfsFileStatus.isDir() && !dump) {
        DirectoryListing listing = dfsClient.listPaths(src, HdfsFileStatus.EMPTY_NAME);
        HdfsFileStatus[] fileList = listing.getPartialListing();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        for (int i = 0; i < fileList.length; i++) {
          appendLog(
              String.format("%s%s %5d %s\t%s\t%13s %s %s", fileList[i].isDir() ? 'd' : '-',
                  fileList[i].getPermission(), fileList[i].getReplication(),
                  fileList[i].getOwner(), fileList[i].getGroup(),
                  human ? readableFileSize(fileList[i].getLen()) : fileList[i].getLen(),
                  formatter.format(fileList[i].getModificationTime()), fileList[i].getFullPath(new Path(src))));
          if (recursively && fileList[i].isDir()) {
            listDirectory(fileList[i].getFullPath(new Path(src)).toString());
          }
        }
      } else {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        appendLog(
            String.format("%s%s %5d %s\t%s\t%13s %s %s", hdfsFileStatus.isDir() ? 'd' : '-',
                hdfsFileStatus.getPermission(),
                hdfsFileStatus.getReplication(),
                hdfsFileStatus.getOwner(), hdfsFileStatus.getGroup(),
                human ? readableFileSize(hdfsFileStatus.getLen()) : hdfsFileStatus.getLen(),
                formatter.format(hdfsFileStatus.getModificationTime()), hdfsFileStatus.getFullPath(new Path(src))));
      }
    } else {
      //list file in remote Directory
      //TODO read conf from files
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(src), conf);
      Path srcPath = new Path(src);
      FileStatus status = fs.getFileStatus(new Path(src));
      if (status == null) {
        appendLog("File not found!");
        return;
      }
      if (status.isDirectory()) {
        RemoteIterator<FileStatus> pathIterator = fs.listStatusIterator(srcPath);
        while (pathIterator.hasNext()) {
          appendLog(String.format("%s", pathIterator.next().getPath()));
        }
      } else {
        appendLog(String.format("%s", src));
      }
    }
  }
}
