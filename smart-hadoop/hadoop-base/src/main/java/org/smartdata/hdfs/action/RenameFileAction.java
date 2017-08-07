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
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionException;
import org.smartdata.actions.Utils;
import org.smartdata.actions.annotation.ActionSignature;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * An action to rename a single file
 * If dest doesn't contains "hdfs" prefix, then destination will be set to
 * current cluster.
 * Note that destination should contains filename.
 */
@ActionSignature(
    actionId = "rename",
    displayName = "rename",
    usage = HdfsAction.FILE_PATH + " $src " + RenameFileAction.DEST_PATH +
        " $dest"
)
public class RenameFileAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CopyFileAction.class);
  public static final String DEST_PATH = "-dest";
  private String srcPath;
  private String destPath;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(DEST_PATH)) {
      this.destPath = args.get(DEST_PATH);
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
        String.format("Action starts at %s : Rename %s to %s",
            Utils.getFormatedCurrentTime(), srcPath, destPath));
    //rename
    renameSingleFile(srcPath, destPath);
  }

  private boolean renameSingleFile(String src,
      String dest) throws IOException, ActionException {
    if (dest.startsWith("hdfs") && src.startsWith("hdfs")) {
      //rename file in the same remote cluster
      // TODO read conf from files
      //check the file name
      if (!URI.create(dest).getHost().equals(URI.create(src).getHost())) {
        throw new ActionException("the file names are not in the same cluster");
      }
      Configuration conf = new Configuration();
      //get FileSystem object
      FileSystem fs = FileSystem.get(URI.create(dest), conf);
      return fs.rename(new Path(src), new Path(dest));
    } else if (!dest.startsWith("hdfs") && !src.startsWith("hdfs")) {
      //rename file in local cluster and overwrite
      if (!dfsClient.exists(src)) {
        throw new ActionException("the source file is not exist");
      }
      dfsClient.rename(src, dest, Options.Rename.NONE);
      return true;
    } else {
      // TODO handle the case when dest prefixed with the default hdfs uri
      // while src not, the two path are in the same cluster
      throw new ActionException("the file names are not in the same cluster");
    }
  }
}
