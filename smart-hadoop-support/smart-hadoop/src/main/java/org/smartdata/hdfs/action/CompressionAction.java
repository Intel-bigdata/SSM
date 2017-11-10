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

import com.google.gson.Gson;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.SmartCompressorStream;
import org.smartdata.model.SmartFileCompressionInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * This action convert a file to a compressed file.
 */
@ActionSignature(
    actionId = "compress",
    displayName = "compress",
    usage =
        HdfsAction.FILE_PATH
            + " $file "
            + CompressionAction.BUF_SIZE
            + " $size "
)
public class CompressionAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompressionAction.class);

  public static final String BUF_SIZE = "-bufSize";

  private String filePath;
  private int bufferSize = 256 * 1024;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.filePath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE)) {
      this.bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
  }

  @Override
  protected void execute() throws Exception {
    if (filePath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    appendLog(
        String.format("Action starts at %s : Read %s", Utils.getFormatedCurrentTime(), filePath));
    if (!dfsClient.exists(filePath)) {
      throw new ActionException("ReadFile Action fails, file doesn't exist!");
    }
    DFSInputStream dfsInputStream = dfsClient.open(filePath);

    // Generate compressed file
    String compressedFileName = filePath + ".ssm_snappy";
    HdfsFileStatus srcFile = dfsClient.getFileInfo(filePath);
    short replication = srcFile.getReplication();
    long blockSize = srcFile.getBlockSize();
    OutputStream compressedOutputStream = dfsClient.create(compressedFileName,
      true, replication, blockSize);
    compress(dfsInputStream, compressedOutputStream);

    // Replace the original file with the compressed file
    dfsClient.delete(filePath);
    dfsClient.rename(compressedFileName, filePath);
  }

  private void compress(InputStream inputStream, OutputStream outputStream) throws IOException {
    SmartFileCompressionInfo compressionInfo = new SmartFileCompressionInfo(
        filePath, bufferSize);
    SmartCompressorStream smartCompressorStream = new SmartCompressorStream(
        outputStream, new SnappyCompressor(bufferSize), bufferSize);
    byte[] buf = new byte[bufferSize * 5];
    while (true) {
      int len = inputStream.read(buf, 0, buf.length);
      if (len <= 0) {
        break;
      }
      smartCompressorStream.write(buf, 0, len);
    }
    smartCompressorStream.finish();
    outputStream.close();

    String compressionInfoJson = new Gson().toJson(compressionInfo);
    appendResult(compressionInfoJson);
  }
}
