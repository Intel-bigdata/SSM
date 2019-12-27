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
import org.apache.hadoop.fs.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.FileState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * This class is used to uncompress file.
 */
@ActionSignature(
    actionId = "uncompress",
    displayName = "uncompress",
    usage = HdfsAction.FILE_PATH
        + " $file "
        + CompressionAction.BUF_SIZE
        + " $bufSize "
)
public class UncompressionAction extends HdfsAction {
  public static final Logger LOG =
      LoggerFactory.getLogger(UncompressionAction.class);
  public static final String COMPRESS_TMP = "-compressTmp";
  private Configuration conf;
  private float progress;
  private String compressTmpPath;
  private String filePath;
  private int buffSize = 64 * 1024;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.filePath = args.get(FILE_PATH);
    // This is a temp path for compressing a file.
    this.compressTmpPath = args.containsKey(COMPRESS_TMP) ?
        args.get(COMPRESS_TMP) : compressTmpPath;
    this.progress = 0.0F;
  }

  protected void execute() throws Exception {
    if (filePath == null) {
      throw new IllegalArgumentException("File path is missing.");
    }
    if (compressTmpPath == null) {
      throw new IllegalArgumentException(
          "Compression tmp path is not specified!");
    }
    FileState fileState = HadoopUtil.getFileState(dfsClient, filePath);
    if (!(fileState instanceof CompressionFileState)) {
      appendLog("The file is already uncompressed!");
      return;
    }
    OutputStream out = null;
    InputStream in = null;
    try {
      // No need to lock the file by append operation,
      // since compressed file cannot be modified.
      out = dfsClient.create(compressTmpPath, true);
      in = dfsClient.open(filePath);
      long length = dfsClient.getFileInfo(filePath).getLen();
      outputUncompressedData(in, out, (int) length);
      // Overwrite the original file with uncompressed data
      dfsClient.rename(compressTmpPath, filePath, Options.Rename.OVERWRITE);
      appendLog("The given file is successfully uncompressed by codec: " +
          ((CompressionFileState) fileState).getCompressionImpl());

    } catch (IOException e) {
      throw new IOException(e);
    } finally {
      if (out != null) {
        out.close();
      }
      if (in != null) {
        in.close();
      }
    }
  }

  private void outputUncompressedData(InputStream in,
      OutputStream out, int length)
      throws IOException {
    byte[] buff = new byte[buffSize];
    int remainSize = length;
    while (remainSize != 0) {
      int copySize = remainSize < buffSize ? remainSize : buffSize;
      int readSize = in.read(buff, 0, copySize);
      if (readSize == -1) {
        break;
      }
      out.write(buff, 0, copySize);
      remainSize -= readSize;
      this.progress = (float) (length - remainSize) / length;
    }
  }

  @Override
  public float getProgress() {
    return this.progress;
  }
}
