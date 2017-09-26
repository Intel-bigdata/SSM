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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.CompatibilityHelperLoader;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * action to truncate file
 */
@ActionSignature(
    actionId = "truncate",
    displayName = "truncate",
    usage = HdfsAction.FILE_PATH + " $src " + TruncateAction.LENGTH + " $length"
)
public class TruncateAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(TruncateAction.class);
  public static final String LENGTH = "-length";

  private String srcPath;
  private long length;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    srcPath = args.get(FILE_PATH);

    this.length = -1;

    if (args.containsKey(LENGTH)) {
      this.length = Long.parseLong(args.get(LENGTH));
    }
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File src is missing.");
    }

    if (length == -1) {
      throw new IllegalArgumentException("Length is missing");
    }

    System.out.println(truncateClusterFile(srcPath, length));
  }

  private boolean truncateClusterFile(String srcFile, long length) throws IOException {
    if (srcFile.startsWith("hdfs")) {
      // TODO read conf from files
      Configuration conf = new Configuration();
      DistributedFileSystem fs = new DistributedFileSystem();
      fs.initialize(URI.create(srcFile), conf);

      //check the length
      long oldLength = fs.getFileStatus(new Path(srcFile)).getLen();

      if (length > oldLength) {
        throw new IllegalArgumentException("Length is illegal");
      } else {
        return CompatibilityHelperLoader.getHelper().truncate(fs, srcPath, length);
      }
    } else {
      long oldLength = dfsClient.getFileInfo(srcFile).getLen();

      if (length > oldLength) {
        throw new IllegalArgumentException("Length is illegal");
      } else {
        return CompatibilityHelperLoader.getHelper().truncate(dfsClient, srcPath, length);
      }
    }
  }
}
