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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.model.FileContainerInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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
  private float status;
  private Configuration conf;
  private String smallFiles;
  private String containerFile;
  private final String HDFS_SCHEME = "hdfs";
  public static final String CONTAINER_FILE = "-containerFile";

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.smallFiles = args.get(FILE_PATH);
    this.containerFile = args.get(CONTAINER_FILE);
  }

  @Override
  protected void execute() throws Exception {
    // Get small file list
    if (smallFiles == null || smallFiles.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid small files: %s.", smallFiles));
    }
    ArrayList<String> smallFileList = new Gson().fromJson(
        smallFiles, new TypeToken<ArrayList<String>>() {
        }.getType());

    // Get offset and output stream of container file
    if (containerFile == null || containerFile.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid container file: %s.", containerFile));
    }
    long offset = exists(containerFile) ? getFileLength(containerFile) : 0L;
    OutputStream out = getOutputStream(containerFile);

    appendLog(String.format("Action starts at %s : compact small files to %s.",
        Utils.getFormatedCurrentTime(), containerFile));
    Map<String, FileContainerInfo> fileContainerInfoMap = new HashMap<>(
        smallFileList.size());
    for (String smallFile : smallFileList) {
      if ((smallFile != null) && !smallFile.isEmpty()
          && exists(smallFile) && getFileLength(smallFile) > 0) {
        try (InputStream in = getInputStream(smallFile)) {
          IOUtils.copyBytes(in, out, 4096);
          long fileLen = getFileLength(smallFile);
          fileContainerInfoMap.put(
              smallFile, new FileContainerInfo(containerFile, offset, fileLen));
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
    appendResult(new Gson().toJson(fileContainerInfoMap));
    appendLog(String.format(
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
   * Get output stream for the specified file.
   */
  private OutputStream getOutputStream(String path) throws IOException {
    if (path.startsWith(HDFS_SCHEME)) {
      FileSystem fs = FileSystem.get(URI.create(path), conf);
      int replication = DFSConfigKeys.DFS_REPLICATION_DEFAULT;
      if (fs.exists(new Path(path))) {
        return fs.append(new Path(path));
      } else {
        return fs.create(new Path(path), true, (short) replication);
      }
    } else {
      if (dfsClient.exists(path)) {
        return CompatibilityHelperLoader.getHelper()
            .getDFSClientAppend(dfsClient, path, 64 * 1024, 0);
      } else {
        return dfsClient.create(path, true);
      }
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

  @Override
  public float getProgress() {
    return this.status;
  }
}
