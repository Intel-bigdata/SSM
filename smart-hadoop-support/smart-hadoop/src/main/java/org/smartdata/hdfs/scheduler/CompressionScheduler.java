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
import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.CompressionAction;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.DecompressionAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CompressionFileInfo;
import org.smartdata.model.FileState;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.LaunchCmdlet;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.smartdata.model.ActionInfo.OLD_FILE_ID;

/**
 * A scheduler for compression/decompression action.
 *
 */
public class CompressionScheduler extends ActionSchedulerService {
  private DFSClient dfsClient;
  private MetaStore metaStore;
  public static final String COMPRESSION_ACTION_ID =
      CompressionAction.class.getAnnotation(ActionSignature.class).actionId();
  public static final String DECOMPRESSION_ACTION_ID =
      DecompressionAction.class.getAnnotation(ActionSignature.class).actionId();

  public static final List<String> actions = Arrays.asList(COMPRESSION_ACTION_ID,
      DECOMPRESSION_ACTION_ID);
  public static String COMPRESS_DIR;
  public static final String COMPRESS_TMP = CompressionAction.COMPRESS_TMP;
  public static final String COMPRESS_TMP_DIR = "compress_tmp/";
  private SmartConf conf;
  private Set<String> fileLock;

  public static final Logger LOG =
      LoggerFactory.getLogger(CompressionScheduler.class);

  public CompressionScheduler(SmartContext context, MetaStore metaStore)
      throws IOException {
    super(context, metaStore);
    this.conf = context.getConf();
    this.metaStore = metaStore;

    String ssmWorkDir = conf.get(
        SmartConfKeys.SMART_WORK_DIR_KEY, SmartConfKeys.SMART_WORK_DIR_DEFAULT);
    CompressionScheduler.COMPRESS_DIR =
        new File(ssmWorkDir, COMPRESS_TMP_DIR).getAbsolutePath();
    this.fileLock = new HashSet<>();
  }

  @Override
  public void init() throws IOException {
    try {
      final URI nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
      dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
    } catch (IOException e) {
      LOG.warn("Failed to create dfsClient.");
    }
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
  public boolean supportCompression(String path) throws MetaStoreException, IOException {
    if (path == null) {
      LOG.warn("File path is not specified.");
      return false;
    }

    if (dfsClient.getFileInfo(path).isDir()) {
      LOG.warn("Compression is not applicable to a directory.");
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

  public boolean supportDecompression(String path) throws MetaStoreException, IOException {
    if (path == null) {
      LOG.warn("File path is not specified!");
      return false;
    }
    // Exclude directory case
    if (dfsClient.getFileInfo(path).isDir()) {
      LOG.warn("Decompression is not applicable to a directory.");
      return false;
    }

    FileState fileState = metaStore.getFileState(path);
    if (fileState instanceof CompressionFileState) {
      return true;
    }
    LOG.warn("A compressed file path should be given!");
    return false;
  }

  private String createTmpName(LaunchAction action) {
    String path = action.getArgs().get(HdfsAction.FILE_PATH);
    String fileName;
    int index = path.lastIndexOf("/");
    if (index == path.length() - 1) {
      index = path.substring(0, path.length() - 1).indexOf("/");
      fileName = path.substring(index + 1, path.length() - 1);
    } else {
      fileName = path.substring(index + 1, path.length());
    }
    /**
     * The dest tmp file is under COMPRESSION_DIR and
     * named by fileName, aidxxx and current time in millisecond with "_" separated
     */
    String tmpName = fileName + "_" + "aid" + action.getActionId() +
        "_" + System.currentTimeMillis();
    return tmpName;
  }

  @Override
  public boolean onSubmit(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      int actionIndex) {
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);

    if (!actions.contains(actionInfo.getActionName())) {
      return false;
    }
    if (fileLock.contains(srcPath)) {
      return false;
    }
    try {
      if (actionInfo.getActionName().equals(COMPRESSION_ACTION_ID) &&
          !supportCompression(srcPath)) {
        return false;
      }
      if (actionInfo.getActionName().equals(DECOMPRESSION_ACTION_ID) &&
          !supportDecompression(srcPath)) {
        return false;
      }

      // TODO remove this part
      CompressionFileState fileState = new CompressionFileState(srcPath,
          FileState.FileStage.PROCESSING);
      metaStore.insertUpdateFileState(fileState);
      return true;
    } catch (MetaStoreException e) {
      LOG.error("Failed to submit action due to metastore exception!", e);
      return false;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  @Override
  public ScheduleResult onSchedule(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      LaunchCmdlet cmdlet, LaunchAction action, int actionIndex) {
    // For compression, add compressTmp argument. This arg is assigned by CompressionScheduler
    // and persisted to MetaStore for easily debugging.
    String tmpName = createTmpName(action);
    action.getArgs().put(COMPRESS_TMP, new File(COMPRESS_DIR, tmpName).getAbsolutePath());
    actionInfo.getArgs().put(COMPRESS_TMP, new File(COMPRESS_DIR, tmpName).getAbsolutePath());
    afterSchedule(actionInfo);
    return ScheduleResult.SUCCESS;
  }

  public void afterSchedule(ActionInfo actionInfo) {
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    // lock the file only if ec or unec action is scheduled
    fileLock.add(srcPath);
    try {
      setOldFileId(actionInfo);
    } catch (Throwable t) {
      // We think it may not be a big issue, so just warn user this issue.
      LOG.warn("Failed in maintaining old fid for taking over old data's temperature.");
    }
  }

  /**
   * Speculate action status and set result accordingly.
   */
  @Override
  public boolean isSuccessfulBySpeculation(ActionInfo actionInfo) {
    String path = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    try {
      FileState fileState = HadoopUtil.getFileState(dfsClient, path);
      FileState.FileType fileType = fileState.getFileType();
      if (actionInfo.getActionName().equals(DECOMPRESSION_ACTION_ID)) {
       return fileType == FileState.FileType.NORMAL;
      }
      // Recover action result for successful compress action.
      if (fileType == FileState.FileType.COMPRESSION) {
        CompressionFileInfo compressionFileInfo =
            new CompressionFileInfo((CompressionFileState) fileState);
        actionInfo.setResult(new Gson().toJson(compressionFileInfo));
        return true;
      }
      return false;
    } catch (IOException e) {
      LOG.warn("Failed to get file state, suppose this action was not " +
          "successfully executed: {}", actionInfo.toString());
      return false;
    }
  }

  /**
   * Set old file id which will be persisted into DB. For action status
   * recovery case, the old file id can be acquired for taking over old file's
   * data temperature.
   */
  private void setOldFileId(ActionInfo actionInfo) throws IOException {
    if (actionInfo.getArgs().get(OLD_FILE_ID) != null &&
        !actionInfo.getArgs().get(OLD_FILE_ID).isEmpty()) {
      return;
    }
    List<Long> oids = new ArrayList<>();
    String path = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    try {
      oids.add(dfsClient.getFileInfo(path).getFileId());
    } catch (IOException e) {
      LOG.warn("Failed to set old fid for taking over data temperature!");
      throw e;
    }
    actionInfo.setOldFileIds(oids);
  }

  @Override
  public void onActionFinished(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
    if (!actionInfo.isFinished()) {
      return;
    }
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    try {
      // Compression Action failed
      if (actionInfo.getActionName().equals(COMPRESSION_ACTION_ID) &&
          !actionInfo.isSuccessful()) {
        // TODO: refactor FileState in order to revert to original state if action failed
        // Currently only converting from normal file to other types is supported, so
        // when action failed, just remove the record of this file from metastore.
        // In current implementation, no record in FileState table means the file is normal type.
        metaStore.deleteFileState(srcPath);
        return;
      }
      // Action execution is successful.
      if (actionInfo.getActionName().equals(COMPRESSION_ACTION_ID)) {
        onCompressActionFinished(actionInfo);
      }
      if (actionInfo.getActionName().equals(DECOMPRESSION_ACTION_ID)) {
        onDecompressActionFinished(actionInfo);
      }
      // Take over access count after successful execution.
      takeOverAccessCount(actionInfo);
    } catch (MetaStoreException e) {
      LOG.error("Compression action failed in metastore!", e);
    } catch (Exception e) {
      LOG.error("Compression action error", e);
    } finally {
      // Remove the record as long as the action is finished.
      fileLock.remove(srcPath);
    }
  }

  /**
   * In rename case, the fid of renamed file is not changed. But sometimes, we need
   * to keep old file's access count and let new file takes over this metric. E.g.,
   * with (un)EC/(de)Compress/(un)Compact action, a new file will overwrite the old file.
   */
  public void takeOverAccessCount(ActionInfo actionInfo) {
    try {
      String filePath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
      assert actionInfo.getOldFileIds().size() == 1;
      long oldFid = actionInfo.getOldFileIds().get(0);
      // The new fid may have not been updated in metastore, so
      // we get it from dfs client.
      long newFid = dfsClient.getFileInfo(filePath).getFileId();
      metaStore.updateAccessCountTableFid(oldFid, newFid);
    } catch (Exception e) {
      LOG.warn("Failed to take over file access count, which can make the " +
          "measure for data temperature inaccurate!", e);
    }
  }

  private void onCompressActionFinished(ActionInfo actionInfo)
      throws MetaStoreException {
    if (!actionInfo.getActionName().equals(COMPRESSION_ACTION_ID)) {
      return;
    }
    Gson gson = new Gson();
    String compressionInfoJson = actionInfo.getResult();
    CompressionFileInfo compressionFileInfo = gson.fromJson(compressionInfoJson,
        new TypeToken<CompressionFileInfo>() {
        }.getType());
    if (compressionFileInfo == null) {
      LOG.error("CompressionFileInfo should NOT be null after successful " +
          "execution!");
      return;
    }
    CompressionFileState compressionFileState =
        compressionFileInfo.getCompressionFileState();
    compressionFileState.setFileStage(FileState.FileStage.DONE);
    // Update metastore and then replace file with compressed one
    metaStore.insertUpdateFileState(compressionFileState);
  }

  private void onDecompressActionFinished(ActionInfo actionInfo)
      throws MetaStoreException {
    if (!actionInfo.getActionName().equals(DECOMPRESSION_ACTION_ID)) {
      return;
    }
    // Delete the record from compression_file table
    metaStore.deleteFileState(actionInfo.getArgs().get(HdfsAction.FILE_PATH));
  }
}