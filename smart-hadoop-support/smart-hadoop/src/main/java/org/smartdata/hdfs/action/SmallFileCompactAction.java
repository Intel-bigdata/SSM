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
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.IOUtils;
import org.smartdata.SmartFilePermission;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;

/**
 * An action to compact small files to a big container file.
 */
@ActionSignature(
    actionId = "compact",
    displayName = "compact",
    usage = HdfsAction.FILE_PATH + " $files "
        + SmallFileCompactAction.CONTAINER_FILE + " $container_file "
)
public class SmallFileCompactAction extends HdfsAction {
  private float status = 0f;
  private Configuration conf = null;
  private String smallFiles = null;
  private String containerFile = null;
  private String containerFilePermission = null;
  private String xAttrName = null;
  private final String HDFS_SCHEME = "hdfs";
  public static final String CONTAINER_FILE = "-containerFile";
  public static final String CONTAINER_FILE_PERMISSION = "-containerFilePermission";

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.xAttrName = conf.get(
        SmartConfKeys.SMART_FILE_STATE_XATTR_NAME_KEY,
        SmartConfKeys.SMART_FILE_STATE_XATTR_NAME_DEFAULT);
    this.smallFiles = args.get(FILE_PATH);
    this.containerFile = args.get(CONTAINER_FILE);
    this.containerFilePermission = args.get(CONTAINER_FILE_PERMISSION);
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

    // Get container file path
    if (containerFile == null || containerFile.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid container file: %s.", containerFile));
    }

    // Get container file permission
    SmartFilePermission filePermission = null;
    if (containerFilePermission != null && !containerFilePermission.isEmpty()) {
      filePermission = new Gson().fromJson(
          containerFilePermission, new TypeToken<SmartFilePermission>() {
          }.getType());
    }

    // Get initial offset and output stream
    // Create container file and set permission if not exists
    long offset;
    OutputStream out;
    if (exists(containerFile)) {
      offset = getFileLength(containerFile);
      out = getAppendOutputStream(containerFile);
    } else {
      out = dfsClient.create(containerFile, true);
      if (filePermission != null) {
        dfsClient.setOwner(
            containerFile, filePermission.getOwner(), filePermission.getGroup());
        dfsClient.setPermission(
            containerFile, new FsPermission(filePermission.getPermission()));
      }
      offset = 0L;
    }

    appendLog(String.format("Action starts at %s : compact small files to %s.",
        Utils.getFormatedCurrentTime(), containerFile));
    for (String smallFile : smallFileList) {
      if ((smallFile != null) && !smallFile.isEmpty()
          && exists(smallFile) && getFileLength(smallFile) > 0) {
        try (InputStream in = getInputStream(smallFile)) {
          // Copy bytes of small file to container file
          IOUtils.copyBytes(in, out, 4096);

          // Truncate small file and add file container info to XAttr
          long fileLen = getFileLength(smallFile);
          FileState compactFileState = new CompactFileState(
              smallFile, new FileContainerInfo(containerFile, offset, fileLen));
          truncate0(smallFile, compactFileState);

          offset += fileLen;
          this.status = (smallFileList.indexOf(smallFile) + 1.0f) / smallFileList.size();
          appendLog(String.format(
              "Compact %s to %s successfully.", smallFile, containerFile));
        } catch (IOException e) {
          if (out != null) {
            out.close();
          }
          throw e;
        }
      }
    }

    if (out != null) {
      out.close();
    }
    appendResult(String.format(
        "Compact all the small files to %s successfully.", containerFile));
  }

  /**
   * Check if exists.
   */
  private boolean exists(String path) throws IOException {
    if (path.startsWith(HDFS_SCHEME)) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      return fs.exists(new Path(path));
    } else {
      return dfsClient.exists(path);
    }
  }

  /**
   * Get length of the specified file.
   */
  private long getFileLength(String path) throws IOException {
    if (path.startsWith(HDFS_SCHEME)) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      return fs.getFileStatus(new Path(path)).getLen();
    } else {
      return dfsClient.getFileInfo(path).getLen();
    }
  }

  /**
   * Get append output stream for the specified file.
   */
  private OutputStream getAppendOutputStream(String path) throws IOException {
    if (path.startsWith(HDFS_SCHEME)) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      return fs.append(new Path(path));
    } else {
      return CompatibilityHelperLoader.getHelper()
          .getDFSClientAppend(dfsClient, path, 64 * 1024, 0);
    }
  }

  /**
   * Get input stream for the specified file.
   */
  private InputStream getInputStream(String path) throws IOException {
    if (path.startsWith(HDFS_SCHEME)) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      return fs.open(new Path(path));
    } else {
      return dfsClient.open(path);
    }
  }

  /**
   * Truncate small file and set XAttr contains file container info.
   */
  private void truncate0(String path, FileState compactFileState) throws IOException {
    // Save original metadata of small file
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(path);
    Map<String, byte[]> xAttr = dfsClient.getXAttrs(path);

    // Delete file
    dfsClient.delete(path, true);

    // Create file
    OutputStream out = dfsClient.create(path, true);
    if (out != null) {
      out.close();
    }

    // Set metadata
    dfsClient.setOwner(path, fileStatus.getOwner(), fileStatus.getGroup());
    dfsClient.setPermission(path, fileStatus.getPermission());
    dfsClient.setReplication(path, fileStatus.getReplication());
    dfsClient.setStoragePolicy(path, "Cold");
    dfsClient.setTimes(path, fileStatus.getAccessTime(),
        dfsClient.getFileInfo(path).getModificationTime());

    for(Map.Entry<String, byte[]> entry : xAttr.entrySet()) {
      dfsClient.setXAttr(path, entry.getKey(), entry.getValue(),
          EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    }

    // Set file container info
    dfsClient.setXAttr(path,
        xAttrName, SerializationUtils.serialize(compactFileState),
        EnumSet.of(XAttrSetFlag.CREATE));

  }

  @Override
  public float getProgress() {
    return this.status;
  }
}
