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
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
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
import org.smartdata.hdfs.CompatibilityHelperLoader;
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
 * This action is used to compress a file.
 */
@ActionSignature(
    actionId = "compress",
    displayName = "compress",
    usage =
        HdfsAction.FILE_PATH
            + " $file "
            + CompressionAction.BUF_SIZE
            + " $bufSize "
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
  private MutableFloat progress;

  // bufferSize is also chunk size.
  // This default value limits the minimum buffer size.
  private int bufferSize = 1024 * 1024;
  private int maxSplit;
  // Can be set in config or action arg.
  private String compressCodec;
  // Specified by user in action arg.
  private int userDefinedBufferSize;
  public static final String XATTR_NAME =
      SmartConstants.SMART_FILE_STATE_XATTR_NAME;

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
    this.filePath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE) && !args.get(BUF_SIZE).isEmpty()) {
      this.userDefinedBufferSize = (int) StringUtil.parseToByte(args.get(BUF_SIZE));
    }
    this.compressCodec = args.get(CODEC) != null ? args.get(CODEC) : compressCodec;
    // This is a temp path for compressing a file.
    this.compressTmpPath = args.containsKey(COMPRESS_TMP) ?
        args.get(COMPRESS_TMP) : compressTmpPath;
    this.progress = new MutableFloat(0.0F);
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

    // Consider directory case.
    if (dfsClient.getFileInfo(filePath).isDir()) {
      appendLog("Compression is not applicable to a directory.");
      return;
    }

    // Generate compressed file
    HdfsFileStatus srcFile = dfsClient.getFileInfo(filePath);
    compressionFileState = new CompressionFileState(filePath, bufferSize, compressCodec);
    compressionFileState.setOriginalLength(srcFile.getLen());

    OutputStream appendOut = null;
    DFSInputStream in = null;
    OutputStream out = null;
    try {
      if (srcFile.getLen() == 0) {
        compressionFileInfo = new CompressionFileInfo(false, compressionFileState);
      } else {
        short replication = srcFile.getReplication();
        long blockSize = srcFile.getBlockSize();
        long fileSize = srcFile.getLen();
        appendLog("File length: " + fileSize);
        bufferSize = getActualBuffSize(fileSize);

        // SmartDFSClient will fail to open compressing file with PROCESSING FileStage
        // set by Compression scheduler. But considering DfsClient may be used, we use
        // append operation to lock the file to avoid any modification.
        appendOut = CompatibilityHelperLoader.getHelper().
            getDFSClientAppend(dfsClient, filePath, bufferSize);
        in = dfsClient.open(filePath);
        out = dfsClient.create(compressTmpPath,
            true, replication, blockSize);

        // Keep storage policy consistent.
        // The below statement is not supported on Hadoop-2.7.3 or CDH-5.10.1
        // String storagePolicyName = dfsClient.getStoragePolicy(filePath).getName();
        byte storagePolicyId = dfsClient.getFileInfo(filePath).getStoragePolicy();
        String storagePolicyName = SmartConstants.STORAGE_POLICY_MAP.get(storagePolicyId);
        if (!storagePolicyName.equals("UNDEF")) {
          dfsClient.setStoragePolicy(compressTmpPath, storagePolicyName);
        }

        compress(in, out);
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
            XATTR_NAME, SerializationUtils.serialize(compressionFileState),
            EnumSet.of(XAttrSetFlag.CREATE));
        // Rename operation is moved from CompressionScheduler.
        // Thus, modification for original file will be avoided.
        dfsClient.rename(compressTmpPath, filePath, Options.Rename.OVERWRITE);
      } else {
        // Add to raw path
        dfsClient.setXAttr(filePath,
            XATTR_NAME, SerializationUtils.serialize(compressionFileState),
            EnumSet.of(XAttrSetFlag.CREATE));
      }
    } catch (IOException e) {
      throw new IOException(e);
    } finally {
      if (appendOut != null) {
        try {
          appendOut.close();
        } catch (IOException e) {
          // Hide the expected exception that the original file is missing.
        }
      }
      if (in != null) {
        in.close();
      }
      if (out != null) {
        out.close();
      }
    }
  }

  private void compress(InputStream inputStream, OutputStream outputStream) throws IOException {
    // We use 'progress' (a percentage) to track compression progress.
    SmartCompressorStream smartCompressorStream = new SmartCompressorStream(
        inputStream, outputStream, bufferSize, compressionFileState, progress);
    smartCompressorStream.convert();
  }

  private int getActualBuffSize(long fileSize) {
    // The capacity of originalPos and compressedPos is maxSplit (1000, by default) in database
    // Calculated by max number of splits.
    int calculatedBufferSize = (int) (fileSize / maxSplit);
    LOG.debug("Calculated buffer size: " + calculatedBufferSize);
    LOG.debug("MaxSplit: " + maxSplit);
    // Determine the actual buffer size
    if (userDefinedBufferSize < bufferSize || userDefinedBufferSize < calculatedBufferSize) {
      if (bufferSize <= calculatedBufferSize) {
        LOG.debug("User defined buffer size is too small, use the calculated buffer size:" +
            calculatedBufferSize);
      } else {
        LOG.debug("User defined buffer size is too small, use the default buffer size:" +
            bufferSize);
      }
    }
    return Math.max(Math.max(userDefinedBufferSize, calculatedBufferSize), bufferSize);
  }

  @Override
  public float getProgress() {
    return (float) this.progress.getValue();
  }
}
