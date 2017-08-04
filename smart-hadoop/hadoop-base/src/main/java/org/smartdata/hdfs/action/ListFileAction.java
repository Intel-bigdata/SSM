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
import org.smartdata.actions.Utils;
import org.smartdata.actions.annotation.ActionSignature;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * An action to list files in a directory.
 */
@ActionSignature(
    actionId = "list",
    displayName = "list",
    usage = HdfsAction.FILE_PATH + " $src"
)
public class ListFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(ListFileAction.class);
  private String srcPath;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
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

  private boolean listDirectory(String src) throws IOException {
    if (!src.startsWith("hdfs")) {
      //list file in local Dir
      HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(src);
      if (hdfsFileStatus.isDir()) {
        DirectoryListing listing = dfsClient.listPaths(src, HdfsFileStatus.EMPTY_NAME);
        HdfsFileStatus[] fileList = listing.getPartialListing();
        for (int i = 0; i < fileList.length; i++) {
          appendLog(
              String.format("%s", fileList[i].getFullPath(new Path(src))));
        }
      } else {
        appendLog(String.format("%s", src));
      }
      return true;
    } else {
      //list file in remote Directory
      //TODO read conf from files
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(src), conf);
      if (fs.isDirectory(new Path(src))) {
        RemoteIterator<FileStatus> pathIterator = fs.listStatusIterator(new Path(src));
        while (pathIterator.hasNext()) {
          appendLog(
              String.format("%s", pathIterator.next().getPath()));
        }
      } else {
        appendLog(String.format("%s", src));
      }
      return true;
    }
  }
}
