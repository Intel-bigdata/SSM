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
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;

/**
 * An action to copy a single file from src to destination.
 * If dest doesn't contains "hdfs" prefix, then destination will be set to
 * current cluster, i.e., copy between dirs in current cluster.
 * Note that destination should contains filename.
 */
@ActionSignature(
    actionId = "append",
    displayName = "append",
    usage = HdfsAction.FILE_PATH + " $src" + AppendFileAction.LENGTH + " $length" + AppendFileAction.BUF_SIZE + " $size"
)
public class AppendFileAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CopyFileAction.class);
  public static final String BUF_SIZE = "-bufSize";
  public static final String LENGTH = "-length";
  private String srcPath;
  private long length = 1024;
  private int bufferSize = 64 * 1024;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE)) {
      bufferSize = Integer.valueOf(args.get(BUF_SIZE));
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
    appendLog(
        String.format("Action starts at %s : Read %s",
            Utils.getFormatedCurrentTime(), srcPath));
    if (!dfsClient.exists(srcPath)) {
      throw new ActionException("CopyFile Action fails, file doesn't exist!");
    }
    appendLog(
        String.format("Append %s", srcPath));
    System.out.println("开始");
    appendRandomFile(srcPath, length, bufferSize);
  }

  private boolean appendRandomFile(String src, long length, int bufferSize) throws IOException {
    if (src.startsWith("hdfs")) {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(src), conf);

      OutputStream outputStream = fs.create(new Path("/appendTestEmptyFile"));

      for (int i = 0; i < 40; i++) {
        outputStream.write(1);
      }

      outputStream.close();


      InputStream inputStream = fs.open(new Path("/appendTestEmptyFile"));

      OutputStream outputStream1 = fs.append(new Path("/appendTestEmptyFile"), bufferSize);

      IOUtils.copyBytes(inputStream, outputStream1, bufferSize, true);

      fs.delete(new Path("/appendTestEmptyFile"), true);
    } else {
      System.out.println("本地");
      OutputStream outputStream = dfsClient.create("/appendTestEmptyFile", true);

      for (int i = 0; i < 40; i++) {
        outputStream.write(1);
      }

      outputStream.close();

      InputStream in = dfsClient.open("/appendTestEmptyFile");
      OutputStream out = dfsClient.append(src, bufferSize, EnumSet.of(CreateFlag.APPEND), null, null);

      IOUtils.copyBytes(in, out, bufferSize, false);
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);

    }

    return true;
  }
}
