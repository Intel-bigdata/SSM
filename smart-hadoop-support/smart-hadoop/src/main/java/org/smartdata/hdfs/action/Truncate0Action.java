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
 * action to set file length to zero
 */
@ActionSignature(
    actionId = "truncate0",
    displayName = "truncate0",
    usage = HdfsAction.FILE_PATH + " $src "
)
public class Truncate0Action extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(TruncateAction.class);
  private String srcPath;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    srcPath = args.get(FILE_PATH);
  }

  @Override
  protected void execute() throws Exception {
    if (srcPath == null) {
      throw new IllegalArgumentException("File src is missing.");
    }
    setLen2Zero(srcPath);
  }

  private boolean setLen2Zero(String srcPath) throws IOException {
    if (srcPath.startsWith("hdfs")) {
      // TODO read conf from files
      Configuration conf = new Configuration();
      DistributedFileSystem fs = new DistributedFileSystem();
      fs.initialize(URI.create(srcPath), conf);

      return CompatibilityHelperLoader.getHelper().setLen2Zero(fs, srcPath);
    } else {
      return CompatibilityHelperLoader.getHelper().setLen2Zero(dfsClient, srcPath);
    }
  }
}
