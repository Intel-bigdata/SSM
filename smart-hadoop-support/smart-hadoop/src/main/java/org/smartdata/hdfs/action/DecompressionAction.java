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
import org.smartdata.SmartConstants;
import org.smartdata.action.ActionException;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.FileState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * This class is used to decompress file.
 */
@ActionSignature(
    actionId = "decompress",
    displayName = "decompress",
    usage = HdfsAction.FILE_PATH
        + " $file "
        + CompressionAction.BUF_SIZE
        + " $bufSize "
)
public class DecompressionAction extends HdfsAction {
  public static final Logger LOG =
      LoggerFactory.getLogger(DecompressionAction.class);
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

    if (!dfsClient.exists(filePath)) {
      throw new ActionException(
          "Failed to execute Compression Action: the given file doesn't exist!");
    }
    // Consider directory case.
    if (dfsClient.getFileInfo(filePath).isDir()) {
      appendLog("Decompression is not applicable to a directory.");
      return;
    }

    FileState fileState = HadoopUtil.getFileState(dfsClient, filePath);
    if (!(fileState instanceof CompressionFileState)) {
      appendLog("The file is already decompressed!");
      return;
    }
    OutputStream out = null;
    InputStream in = null;
    try {
      // No need to lock the file by append operation,
      // since compressed file cannot be modified.
      out = dfsClient.create(compressTmpPath, true);

      // Keep storage policy consistent.
      // The below statement is not supported on Hadoop-2.7.3 or CDH-5.10.1
      // String storagePolicyName = dfsClient.getStoragePolicy(filePath).getName();
      byte storagePolicyId = dfsClient.getFileInfo(filePath).getStoragePolicy();
      String storagePolicyName = SmartConstants.STORAGE_POLICY_MAP.get(storagePolicyId);
      if (!storagePolicyName.equals("UNDEF")) {
        dfsClient.setStoragePolicy(compressTmpPath, storagePolicyName);
      }

      in = dfsClient.open(filePath);
      long length = dfsClient.getFileInfo(filePath).getLen();
      outputDecompressedData(in, out, length);
      // Overwrite the original file with decompressed data
      dfsClient.setOwner(compressTmpPath, dfsClient.getFileInfo(filePath).getOwner(), dfsClient.getFileInfo(filePath).getGroup());
      dfsClient.setPermission(compressTmpPath, dfsClient.getFileInfo(filePath).getPermission());
      dfsClient.rename(compressTmpPath, filePath, Options.Rename.OVERWRITE);
      appendLog("The given file is successfully decompressed by codec: " +
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

  private void outputDecompressedData(InputStream in, OutputStream out,
      long length) throws IOException {
    byte[] buff = new byte[buffSize];
    long remainSize = length;
    while (remainSize != 0) {
      int copySize = remainSize < buffSize ? (int) remainSize : buffSize;
      // readSize may be smaller than copySize. Here, readSize is the actual
      // number of bytes read to buff.
      int readSize = in.read(buff, 0, copySize);
      if (readSize == -1) {
        break;
      }
      // Use readSize instead of copySize.
      out.write(buff, 0, readSize);
      remainSize -= readSize;
      this.progress = (float) (length - remainSize) / length;
    }
  }

  @Override
  public float getProgress() {
    return this.progress;
  }
}
