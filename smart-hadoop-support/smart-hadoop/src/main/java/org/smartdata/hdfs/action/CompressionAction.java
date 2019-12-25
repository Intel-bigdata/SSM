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
import org.apache.hadoop.hdfs.CompressionCodec;
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
import org.smartdata.utils.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
            + CompressionAction.CODEC
            + " $codec"
)
public class CompressionAction extends HdfsAction {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompressionAction.class);

  public static final String BUF_SIZE = "-bufSize";
  public static final String CODEC = "-codec";
  private static List<String> compressionCodecList = CompressionCodec.CODEC_LIST;

  private String filePath;
  private Configuration conf;

  // bufferSize is also chunk size.
  // This default value limits the minimum buffer size.
  private int bufferSize = 1024 * 1024;
  private int maxSplit;
  // Can be set in config or action arg.
  private String compressCodec;
  // Specified by user in action arg.
  private int userDefinedBufferSize;
  // Calculated by max number of splits.
  private int calculatedBufferSize;
  private String xAttrName = null;

  private CompressionFileInfo compressionFileInfo;
  private CompressionFileState compressionFileState;

  private String compressTmpPath;
  public static final String COMPRESS_TMP = "-compressTmp";

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.compressCodec = conf.get(
        SmartConfKeys.SMART_COMPRESSION_CODEC,
        SmartConfKeys.SMART_COMPRESSION_CODEC_DEFAULT);
    this.maxSplit = conf.getInt(
        SmartConfKeys.SMART_COMPRESSION_MAX_SPLIT,
        SmartConfKeys.SMART_COMPRESSION_MAX_SPLIT_DEFAULT);
    this.xAttrName = SmartConstants.SMART_FILE_STATE_XATTR_NAME;
    this.filePath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE) && !args.get(BUF_SIZE).isEmpty()) {
      this.userDefinedBufferSize = (int) StringUtil.parseToByte(args.get(BUF_SIZE));
    }
    this.compressCodec = args.get(CODEC) != null ? args.get(CODEC) : compressCodec;
    // This is a temp path for compressing a file.
    this.compressTmpPath = args.containsKey(COMPRESS_TMP) ?
        args.get(COMPRESS_TMP) : compressTmpPath;
  }

  @Override
  protected void execute() throws Exception {
    if (filePath == null) {
      throw new IllegalArgumentException("File path is missing.");
    }
    if (compressTmpPath == null) {
      throw new IllegalArgumentException("Compression tmp path is not specified!");
    }
    if (!compressionCodecList.contains(compressCodec)) {
      throw new ActionException(
          "Compression Action failed due to unsupported codec: " + compressCodec);
    }
    appendLog(
        String.format("Compression Action started at %s for %s",
            Utils.getFormatedCurrentTime(), filePath));

    if (!dfsClient.exists(filePath)) {
      throw new ActionException(
          "Failed to execute Compression Action: the given file doesn't exist!");
    }
    // Generate compressed file
    HdfsFileStatus srcFile = dfsClient.getFileInfo(filePath);
    compressionFileState = new CompressionFileState(filePath, bufferSize, compressCodec);
    compressionFileState.setOriginalLength(srcFile.getLen());
    if (srcFile.getLen() == 0) {
      compressionFileInfo = new CompressionFileInfo(false, compressionFileState);
    } else {
      short replication = srcFile.getReplication();
      long blockSize = srcFile.getBlockSize();
      long fileSize = srcFile.getLen();
      appendLog("File length: " + fileSize);
      //The capacity of originalPos and compressedPos is maxSplit (1000, by default) in database
      this.calculatedBufferSize = (int) (fileSize / maxSplit);
      LOG.debug("Calculated buffer size: " + calculatedBufferSize);
      LOG.debug("MaxSplit: " + maxSplit);
      //Determine the actual buffer size
      if (userDefinedBufferSize < bufferSize || userDefinedBufferSize < calculatedBufferSize) {
        if (bufferSize <= calculatedBufferSize) {
          LOG.debug("User defined buffer size is too small, use the calculated buffer size:" +
              calculatedBufferSize);
        } else {
          LOG.debug("User defined buffer size is too small, use the default buffer size:" +
              bufferSize);
        }
      }
      bufferSize = Math.max(Math.max(userDefinedBufferSize, calculatedBufferSize), bufferSize);

      DFSInputStream dfsInputStream = dfsClient.open(filePath);

      OutputStream compressedOutputStream = dfsClient.create(compressTmpPath,
          true, replication, blockSize);
      compress(dfsInputStream, compressedOutputStream);
      HdfsFileStatus destFile = dfsClient.getFileInfo(compressTmpPath);
      compressionFileState.setCompressedLength(destFile.getLen());
      appendLog("Compressed file length: " + destFile.getLen());
      compressionFileInfo =
          new CompressionFileInfo(true, compressTmpPath, compressionFileState);
    }
    compressionFileState.setBufferSize(bufferSize);
    appendLog("Compression buffer size: " + bufferSize);
    appendLog("Compression codec: " + compressCodec);
    String compressionInfoJson = new Gson().toJson(compressionFileInfo);
    appendResult(compressionInfoJson);
    LOG.warn(compressionInfoJson);
    if (compressionFileInfo.needReplace()) {
      // Add to temp path
      // Please make sure content write to Xatte is less than 64K
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
