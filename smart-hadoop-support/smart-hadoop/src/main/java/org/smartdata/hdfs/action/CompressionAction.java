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
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.SmartCompressorStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.action.ActionException;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.CompressionFileInfo;
import org.smartdata.model.CompressionFileState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
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
            + CompressionAction.COMPRESS_IMPL
            + " $impl"
)
public class CompressionAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompressionAction.class);

  public static final String BUF_SIZE = "-bufSize";
  public static final String COMPRESS_IMPL = "-compressImpl";
  private static List<String> compressionImplList = Arrays.asList("Lz4","Bzip2","Zlib","snappy");

  private String filePath;
  private Configuration conf;

  private int bufferSize = 1024 * 1024;
  private int maxSplit;
  private String compressionImpl;
  private int UserDefinedbuffersize;
  private int Calculatedbuffersize;
  private String xAttrName = null;

  private CompressionFileInfo compressionFileInfo;
  private CompressionFileState compressionFileState;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.compressionImpl = conf.get(
        SmartConfKeys.SMART_COMPRESSION_IMPL,
        SmartConfKeys.SMART_COMPRESSION_IMPL_DEFAULT);
    this.maxSplit = conf.getInt(
        SmartConfKeys.SMART_COMPRESSION_MAX_SPLIT,
        SmartConfKeys.SMART_COMPRESSION_MAX_SPLIT_DEFAULT);
    this.xAttrName = SmartConstants.SMART_FILE_STATE_XATTR_NAME;

    this.filePath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE)) {
      this.UserDefinedbuffersize = Integer.valueOf(args.get(BUF_SIZE));
    }
    if (args.containsKey(COMPRESS_IMPL)) {
      this.compressionImpl = args.get(COMPRESS_IMPL);
    }
  }

  @Override
  protected void execute() throws Exception {
    if (filePath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    if (!compressionImplList.contains(compressionImpl)) {
      throw new ActionException("Action fails, this compressionImpl isn't supported!");
    }
    appendLog(
        String.format("Action starts at %s : Read %s", Utils.getFormatedCurrentTime(), filePath));

    if (!defaultDfsClient.exists(filePath)) {
      throw new ActionException("ReadFile Action fails, file doesn't exist!");
    }
    // Generate compressed file
    HdfsFileStatus srcFile = defaultDfsClient.getFileInfo(filePath);
    compressionFileState = new CompressionFileState(filePath, bufferSize, compressionImpl);
    compressionFileState.setOriginalLength(srcFile.getLen());
    if (srcFile.getLen() == 0) {
      compressionFileInfo = new CompressionFileInfo(false, compressionFileState);
    } else {
      String tempPath = "/tmp/ssm" + filePath + "." + System.currentTimeMillis() + ".ssm_compression";
      short replication = srcFile.getReplication();
      long blockSize = srcFile.getBlockSize();
      long fileSize = srcFile.getLen();
      appendLog("File length: " + fileSize);
      //The capacity of originalPos and compressedPos is maxSplit (3000) in database
      this.Calculatedbuffersize = (int) (fileSize / maxSplit);
      appendLog("Calculatedbuffersize: " + Calculatedbuffersize);
      appendLog("MaxSplit: " + maxSplit);
      //Determine the actual buffersize
      if (UserDefinedbuffersize < bufferSize || UserDefinedbuffersize < Calculatedbuffersize) {
        if (bufferSize <= Calculatedbuffersize) {
          appendLog("User defined buffersize is too small,use the calculated buffersize:" + Calculatedbuffersize);
        } else {
          appendLog("User defined buffersize is too small,use the default buffersize:" + bufferSize);
        }
      }
      bufferSize = Math.max(Math.max(UserDefinedbuffersize, Calculatedbuffersize), bufferSize);

      DFSInputStream dfsInputStream = defaultDfsClient.open(filePath);

      OutputStream compressedOutputStream = defaultDfsClient.create(tempPath,
          true, replication, blockSize);
      compress(dfsInputStream, compressedOutputStream);
      HdfsFileStatus destFile = defaultDfsClient.getFileInfo(tempPath);
      compressionFileState.setCompressedLength(destFile.getLen());
      compressionFileInfo = new CompressionFileInfo(true, tempPath, compressionFileState);
    }
    compressionFileState.setBufferSize(bufferSize);
    String compressionInfoJson = new Gson().toJson(compressionFileInfo);
    appendResult(compressionInfoJson);
    LOG.warn(compressionInfoJson);
    if (compressionFileInfo.needReplace()) {
      // Add to temp path
      dfsClient.setXAttr(compressionFileInfo.getTempPath(),
          xAttrName, SerializationUtils.serialize(compressionFileState),
          EnumSet.of(XAttrSetFlag.CREATE));
    } else {
      // Add to raw path
      dfsClient.setXAttr(filePath,
          xAttrName, SerializationUtils.serialize(compressionFileState),
          EnumSet.of(XAttrSetFlag.CREATE));
    }
  }

  private void compress(InputStream inputStream, OutputStream outputStream) throws IOException {
    SmartCompressorStream smartCompressorStream = new SmartCompressorStream(
        inputStream, outputStream, bufferSize, compressionFileState);
    smartCompressorStream.convert();
  }
}
