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
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.Utils;
import org.smartdata.actions.annotation.ActionSignature;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;

/**
 * action to Merge File
 */
@ActionSignature(
    actionId = "merge",
    displayName = "merge",
    usage = HdfsAction.FILE_PATH + "  $src " + MergeFileAction.DEST_PATH + " $dest " +
        MergeFileAction.BUF_SIZE + " $size"
)
public class MergeFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(MergeFileAction.class);
  public static final String DEST_PATH = "-dest";
  public static final String BUF_SIZE = "-bufSize";
  private LinkedList<String> srcPathList;
  private int bufferSize = 64 * 1024;
  private String target;


  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    String allSrcPath = args.get(FILE_PATH);

    String[] allSrcPathArr = allSrcPath.split(",");
    srcPathList = new LinkedList<String>(Arrays.asList(allSrcPathArr));

    if (args.containsKey(DEST_PATH)) {
      this.target = args.get(DEST_PATH);
    }
    if (args.containsKey(BUF_SIZE)) {
      bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
  }

  @Override
  protected void execute() throws Exception {
    if (srcPathList == null || srcPathList.size() == 0) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    if (target == null) {
      throw new IllegalArgumentException("Dest File parameter is missing.");
    }
    if (srcPathList.size() == 1) {
      throw new IllegalArgumentException("Don't accept only one source file");
    }

    appendLog(
        String.format("Action starts at %s : Merge %s to %s",
            Utils.getFormatedCurrentTime(), srcPathList, target));

    //Merge
    mergeFiles(srcPathList,target);
  }

  private boolean mergeFiles(LinkedList<String> srcFiles, String dest) throws IOException {
    InputStream srcInputStream = null;
    OutputStream destInputStream = getTargetOutputStream(dest);
    for (String srcEle : srcPathList) {
      srcInputStream = getSourceInputStream(srcEle);
      IOUtils.copyBytes(srcInputStream, destInputStream, bufferSize, false);
      IOUtils.closeStream(srcInputStream);
    }
    IOUtils.closeStream(destInputStream);
    return true;
  }

  private InputStream getSourceInputStream(String src) throws IOException {
    if (src.startsWith("hdfs")) {
      //get stream of source
      // TODO read conf from files
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(src), conf);
      return fs.open(new Path(src));
    } else {
      return dfsClient.open(src);
    }
  }

  private OutputStream getTargetOutputStream(String dest) throws IOException {
    if (dest.startsWith("hdfs")) {
      // TODO read conf from files
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(dest), conf);
      if (fs.exists(new Path(target))) {
        fs.delete(new Path(target), true);
      }
      return fs.create(new Path(dest), true);
    } else {
      if (dfsClient.exists(target)) {
        dfsClient.delete(target, true);
      }
      return dfsClient.create(dest, true);
    }
  }
}
