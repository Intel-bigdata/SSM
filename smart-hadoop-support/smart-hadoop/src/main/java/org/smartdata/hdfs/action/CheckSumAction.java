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

import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.HadoopUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Map;

@ActionSignature(
    actionId = "checksum",
    displayName = "checksum",
    usage = HdfsAction.FILE_PATH + " $src "
)
public class CheckSumAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(CheckSumAction.class);
  private String fileName;
  private SmartConf conf;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    fileName = args.get(FILE_PATH);
    this.conf = getContext().getConf();
  }

  @Override
  protected void execute() throws Exception {
    // Use pre-set SmartDFSClient.
    // this.setDfsClient(HadoopUtil.getDFSClient(
    //    HadoopUtil.getNameNodeUri(conf), conf));
    if (fileName == null) {
      throw new IllegalArgumentException("Please specify file path!");
    }

    if (fileName.charAt(fileName.length() - 1) == '*') {
      DirectoryListing listing = dfsClient.listPaths(fileName.substring(0,
          fileName.length() - 1), HdfsFileStatus.EMPTY_NAME);
      HdfsFileStatus[] fileList = listing.getPartialListing();
      for (HdfsFileStatus fileStatus : fileList) {
        String file1 = fileStatus.getFullPath(new Path(
            fileName.substring(0, fileName.length() - 1))).toString();
        HdfsFileStatus fileStatus1 = dfsClient.getFileInfo(file1);
        long length = fileStatus1.getLen();
        MD5MD5CRC32FileChecksum md5 = dfsClient.getFileChecksum(file1, length);

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(byteStream);
        md5.write(dataStream);
        byte[] bytes = byteStream.toByteArray();
        appendLog(
            String.format("%s\t%s\t%s",
                file1,
                md5.getAlgorithmName(),
                byteArray2HexString(bytes)
            ));
      }
      return;
    }

    HdfsFileStatus fileStatus = dfsClient.getFileInfo(fileName);
    if (fileStatus != null) {
      if (fileStatus.isDir()) {
        appendResult("This is a directory which has no checksum result!");
        appendLog("This is a directory which has no checksum result!");
        return;
      }
    }

    long length = fileStatus.getLen();
    MD5MD5CRC32FileChecksum md5 = dfsClient.getFileChecksum(fileName, length);
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(byteStream);
    md5.write(dataStream);

    byte[] bytes = byteStream.toByteArray();

    appendLog(
        String.format("%s\t%s\t%s",
            fileName,
            md5.getAlgorithmName(),
            byteArray2HexString(bytes)
        ));
  }

  public static String byteArray2HexString(byte[] bytes) {
    if (bytes == null || bytes.length <= 0) {
      return null;
    }
    char[] chars = new char[bytes.length * 2];
    final char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
        '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    for (int i = 0, j = 0; i < bytes.length; i++) {
      chars[j++] = hexDigits[bytes[i] >> 4 & 0x0f];
      chars[j++] = hexDigits[bytes[i] & 0x0f];
    }
    return new String(chars);
  }
}
