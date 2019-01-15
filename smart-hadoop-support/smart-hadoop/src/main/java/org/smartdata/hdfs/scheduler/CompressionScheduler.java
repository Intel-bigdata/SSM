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
package org.smartdata.hdfs.scheduler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CompressionFileInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.CompressionFileState;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * CompressionScheduler.
 */
public class CompressionScheduler extends ActionSchedulerService {
  private DFSClient dfsClient;
  private final URI nnUri;
  private MetaStore metaStore;
  private static final List<String> actions = Arrays.asList("compress");

  public static final Logger LOG =
      LoggerFactory.getLogger(CompressionScheduler.class);

  public CompressionScheduler(SmartContext context, MetaStore metaStore)
      throws IOException {
    super(context, metaStore);
    this.metaStore = metaStore;
    nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
  }

  @Override
  public void init() throws IOException {
    dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public List<String> getSupportedActions() {
    return actions;
  }

  /**
   * Check if the file type support compression action.
   *
   * @param path
   * @return true if the file supports compression action, else false
   */
  private boolean supportCompression(String path) throws MetaStoreException {
    if (path == null) {
      LOG.warn("File is not specified.");
      return false;
    }
    // Current implementation: only normal file type supports compression action
    FileState fileState = metaStore.getFileState(path);
    if (fileState.getFileType().equals(FileState.FileType.NORMAL)
        && fileState.getFileStage().equals(FileState.FileStage.DONE)) {
      return true;
    }
    LOG.debug("File " + path + " doesn't support compression action. "
        + "Type: " + fileState.getFileType() + "; Stage: " + fileState.getFileStage());
    return false;
  }

  @Override
  public boolean onSubmit(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
    String path = actionInfo.getArgs().get("-file");
    try {
      if (!supportCompression(path)) {
        return false;
      }
      // TODO remove this part
      CompressionFileState fileState = new CompressionFileState(path,
          FileState.FileStage.PROCESSING);
      metaStore.insertUpdateFileState(fileState);
      return true;
    } catch (MetaStoreException e) {
      LOG.error("Compress action of file " + path + " failed in metastore!", e);
      return false;
    }
  }

  @Override
  public void onActionFinished(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
    if (actionInfo.isFinished()) {
      try {
        // Action failed
        if (!actionInfo.isSuccessful()) {
          // TODO: refactor FileState in order to revert to original state if action failed
          // Currently only converting from normal file to other types is supported, so
          // when action failed, just remove the record of this file from metastore.
          // In current implementation, no record in FileState table means the file is normal type.
          metaStore.deleteFileState(actionInfo.getArgs().get("-file"));
        } else {
        // Action successful
          Gson gson = new Gson();
          String compressionInfoJson = actionInfo.getResult();
          CompressionFileInfo compressionFileInfo = gson.fromJson(compressionInfoJson,
              new TypeToken<CompressionFileInfo>() {
              }.getType());
          boolean needReplace = compressionFileInfo.needReplace();
          String path = actionInfo.getArgs().get("-file");
          String tempPath = compressionFileInfo.getTempPath();
          CompressionFileState compressionFileState = compressionFileInfo.getCompressionFileState();
          compressionFileState.setFileStage(FileState.FileStage.DONE);
          // Update metastore and then replace file with compressed one
          metaStore.insertUpdateFileState(compressionFileState);
          if (needReplace) {
            dfsClient.rename(tempPath, path, Options.Rename.OVERWRITE);
          }
        }
      } catch (MetaStoreException e) {
        LOG.error("Compression action failed in metastore!", e);
      } catch (Exception e) {
        LOG.error("Compression action error", e);
      }
    }
  }
}