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
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.IOUtils;
import org.smartdata.SmartConstants;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.HadoopUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;

/**
 * An action to recovery contents of compacted ssm small files.
 */
@ActionSignature(
    actionId = "uncompact",
    displayName = "uncompact",
    usage = SmallFileUncompactAction.CONTAINER_FILE + " $container_file "
)
public class SmallFileUncompactAction extends HdfsAction {
  private float status = 0f;
  private Configuration conf = null;
  private String smallFiles = null;
  private String xAttrNameFileState = null;
  private String xAttrNameCheckSum = null;
  private String containerFile = null;
  private DFSClient smartDFSClient = null;
  public static final String CONTAINER_FILE = "-containerFile";

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.smartDFSClient = dfsClient;
    this.xAttrNameFileState = SmartConstants.SMART_FILE_STATE_XATTR_NAME;
    this.xAttrNameCheckSum = SmartConstants.SMART_FILE_CHECKSUM_XATTR_NAME;
    this.smallFiles = args.get(FILE_PATH);
    this.containerFile = args.get(CONTAINER_FILE);
  }

  @Override
  protected void execute() throws Exception {
    // Set hdfs client by DFSClient rather than SmartDFSClient
    this.setDfsClient(HadoopUtil.getDFSClient(
        HadoopUtil.getNameNodeUri(conf), conf));

    // Get small file list
    if (smallFiles == null || smallFiles.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid small files: %s.", smallFiles));
    }
    ArrayList<String> smallFileList = new Gson().fromJson(
        smallFiles, new TypeToken<ArrayList<String>>() {
        }.getType());
    if (smallFileList == null || smallFileList.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid small files: %s.", smallFiles));
    }

    // Get container file path
    if (containerFile == null || containerFile.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid container file: %s.", containerFile));
    }
    appendLog(String.format(
        "Action starts at %s : uncompact small files.",
        Utils.getFormatedCurrentTime()));

    for (String smallFile : smallFileList) {
      if ((smallFile != null) && !smallFile.isEmpty()
          && dfsClient.exists(smallFile)) {
        DFSInputStream in = null;
        OutputStream out = null;
        try {
          // Get compact input stream
          in = smartDFSClient.open(smallFile);

          // Save original metadata of small file and delete original small file
          HdfsFileStatus fileStatus = dfsClient.getFileInfo(smallFile);
          Map<String, byte[]> xAttr = dfsClient.getXAttrs(smallFile);
          dfsClient.delete(smallFile, false);

          // Create new small file
          out = dfsClient.create(smallFile, true);

          // Copy contents to original small file
          IOUtils.copyBytes(in, out, 4096);

          // Reset file meta data
          resetFileMeta(smallFile, fileStatus, xAttr);

          // Set status and update log
          this.status = (smallFileList.indexOf(smallFile) + 1.0f)
              / smallFileList.size();
          appendLog(String.format("Uncompact %s successfully.", smallFile));
        } finally {
          if (in != null) {
            in.close();
          }
          if (out != null) {
            out.close();
          }
        }
      }
    }

    dfsClient.delete(containerFile, false);
    appendLog(String.format("Uncompact all the small files of %s successfully.", containerFile));
  }

  /**
   * Reset meta data of small file. We should exclude the setting for
   * xAttrNameFileState or xAttrNameCheckSum.
   */
  private void resetFileMeta(String path, HdfsFileStatus fileStatus,
      Map<String, byte[]> xAttr) throws IOException {
    dfsClient.setOwner(path, fileStatus.getOwner(), fileStatus.getGroup());
    dfsClient.setPermission(path, fileStatus.getPermission());

    for(Map.Entry<String, byte[]> entry : xAttr.entrySet()) {
      if (!entry.getKey().equals(xAttrNameFileState) &&
          !entry.getKey().equals(xAttrNameCheckSum)) {
        dfsClient.setXAttr(path, entry.getKey(), entry.getValue(),
            EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
      }
    }
  }

  @Override
  public float getProgress() {
    return this.status;
  }
}
