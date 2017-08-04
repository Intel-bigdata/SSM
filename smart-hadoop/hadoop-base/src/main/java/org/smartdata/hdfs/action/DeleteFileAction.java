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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionException;
import org.smartdata.actions.Utils;
import org.smartdata.actions.annotation.ActionSignature;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * An action to delete a single file in dest
 * If dest doesn't contains "hdfs" prefix, then destination will be set to
 * current cluster, i.e., delete file in current cluster.
 * Note that destination should contains filename.
 */
@ActionSignature(
    actionId = "delete",
    displayName = "delete",
    usage = HdfsAction.FILE_PATH + " $file"
)

public class DeleteFileAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteFileAction.class);
  private String filePath;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.filePath = args.get(FILE_PATH);
  }

  @Override
  protected void execute() throws Exception {
    if (filePath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    appendLog(
        String.format("Action starts at %s : Delete %s",
            Utils.getFormatedCurrentTime(), filePath));
    //delete File
    deleteFile(filePath);
  }

  private boolean deleteFile(
      String filePath) throws IOException, ActionException {
    if (filePath.startsWith("hdfs")) {
      //delete in remote cluster
      // TODO read conf from file
      Configuration conf = new Configuration();
      //get FileSystem object
      FileSystem fs = FileSystem.get(URI.create(filePath), conf);
      if (!fs.exists(new Path(filePath))) {
        throw new ActionException(
            "DeleteFile Action fails, file doesn't exist!");
      }
      fs.delete(new Path(filePath), true);
      return true;
    } else {
      //delete in local cluster
      if (!dfsClient.exists(filePath)) {
        throw new ActionException(
            "DeleteFile Action fails, file doesn't exist!");
      }
      appendLog(String.format("Delete %s", filePath));
      return dfsClient.delete(filePath, true);
    }
  }
}


