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
package org.smartdata.server.engine;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.metaservice.CmdletMetaService;
import org.smartdata.metaservice.CopyMetaService;
import org.smartdata.metaservice.MetaServiceException;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CopyScheduler extends AbstractService {
  static final Logger LOG = LoggerFactory.getLogger(CopyScheduler.class);

  private ScheduledExecutorService executorService;

  private CmdletManager cmdletManager;
  private DFSClient dfsClient;

  private CopyMetaService copyMetaService;
  private CmdletMetaService cmdletMetaService;

  private Queue<FileDiff> pendingDR;
  // <cid, did> Map set
  private Map<Long, Long> runningDR;
  private String srcBase;
  private String destBase;
  // TODO currently set max running list.size == 1 for test
  private final int MAX_RUNNING_SIZE = 5;

  public CopyScheduler(ServerContext context) {
    super(context);

    this.executorService = Executors.newSingleThreadScheduledExecutor();

    this.copyMetaService = (CopyMetaService) context.getMetaService();
    this.cmdletMetaService = (CmdletMetaService) context.getMetaService();

    this.runningDR = new HashMap<>();
    this.pendingDR = new LinkedBlockingQueue<>();
  }

  public int getQueueSize() {
    return runningDR.size() + pendingDR.size();
  }

  public CopyScheduler(ServerContext context, CmdletManager cmdletManager,
      DFSClient dfsClient, String srcBase, String destBase) {
    this(context);
    this.cmdletManager = cmdletManager;
    this.destBase = destBase;
    this.srcBase = srcBase;
    this.dfsClient = dfsClient;
  }

  public void diffMerge(List<FileDiff> fileDiffList) {
    // TODO merge diffs and resolve conflicts
  }

  @Override
  public void init() throws IOException {

  }

  @Override
  public void start() throws IOException {
    executorService.scheduleAtFixedRate(
        new ScheduleTask(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() throws IOException {
    executorService.shutdown();
  }

  /**
   * @param sourceFile the source file we want to copy
   * @param destFile   destination to save the different chunk of source file
   * @return a list of copy task
   */
  static public List<CopyTargetTask> splitCopyFile(String sourceFile,
      String destFile, int blockPerchunk, FileSystem fileSystem)
      throws IOException {

    if (blockPerchunk <= 0) {
      throw new IllegalArgumentException(
          "the block per chunk must more than 0");
    }
    if (sourceFile == null) {
      throw new IllegalArgumentException("the source file can't be empty");
    }
    if (destFile == null) {
      throw new IllegalArgumentException("the dest file can't be empty");
    }

    List<CopyTargetTask> copyTargetTaskList = new LinkedList<>();

    //split only in source file is large than a chunk
    if ((blockPerchunk > 0) &&
        !fileSystem.getFileStatus(new Path(sourceFile)).isDirectory() &&
        (fileSystem.getFileStatus(new Path(sourceFile)).getLen() >
            fileSystem.getFileStatus(new Path(sourceFile)).getBlockSize() *
                blockPerchunk)) {
      //here we can split
      final BlockLocation[] blockLocations;
      blockLocations = fileSystem
          .getFileBlockLocations(fileSystem.getFileStatus(new Path(sourceFile)),
              0,
              fileSystem.getFileStatus(new Path(sourceFile)).getLen());

      int numBlocks = blockLocations.length;
      if (numBlocks <= blockPerchunk) {
        //if has only one chunk
        copyTargetTaskList.add(new CopyTargetTask(destFile, sourceFile, 0,
            fileSystem.getFileStatus(new Path(sourceFile)).getLen()));
      } else {
        //has more than one chunk
        int i = 0;
        int chunkCount = 0;
        int position = 0;
        while (i < numBlocks) {
          //the length we need to set in copy task
          long curLength = 0;
          for (int j = 0; j < blockPerchunk && i < numBlocks; ++j, ++i) {
            curLength += blockLocations[i].getLength();
          }
          if (curLength > 0) {
            chunkCount++;
            CopyTargetTask task =
                new CopyTargetTask(destFile + "_temp_chunkCount" + chunkCount,
                    sourceFile,
                    position, curLength);
            copyTargetTaskList.add(task);
            position += curLength;
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Incorrect input");
    }
    return copyTargetTaskList;
  }

  public static String cmdParsing(FileDiff fileDiff, String srcBase,
      String destBase) {
    // Create and Write
    // TODO should not use string parsing
    if (fileDiff.getDiffType() == FileDiffType.CREATE || fileDiff.getDiffType() == FileDiffType.APPEND ) {
      String cmd = String.format("copy %s", fileDiff.getParameters());
      // Locate -file
      int start = cmd.indexOf("-file");
      if (start < 0) {
        return "";
      }
      start += 6;
      int end = cmd.indexOf(' ', start);
      if (end < 0) {
        end = cmd.length();
      }
      String localPath = cmd.substring(start, end);
      String destPath = localPath.replace(srcBase, destBase);
      if (end == cmd.length()) {
        return String.format("%s -dest %s", cmd, destPath);
      } else {
        return String.format("%s -dest %s %s", cmd.substring(0, end), destPath, cmd.substring(end + 1));
      }
    } else {
      String cmd = String.format("rename %s", fileDiff.getParameters());
      // Locate -dest
      return cmd.replace(srcBase, destBase);
    }
  }

  public void forceSync(String src, String dest) throws IOException, MetaServiceException {
    // TODO check dest statuses to avoid unnecessary copy
    // Force Sync src and dest
    dfsClient.getFileInfo(src);
    HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(src);
    if (hdfsFileStatus.isDir()) {
      // Get file list
      DirectoryListing listing = dfsClient.listPaths(src, HdfsFileStatus.EMPTY_NAME);
      HdfsFileStatus[] fileList = listing.getPartialListing();
      for (int i = 0; i < fileList.length; i++) {
        // Recursively insert to file_diff
        forceSync(fileList[i].getFullName(src), fileList[i].getFullName(dest));
      }
    } else {
      // Insert to fill_diff
      FileDiff fileDiff = new FileDiff();
      fileDiff.setDiffType(FileDiffType.APPEND);
      fileDiff.setParameters(String.format("-file %s -dest %s", src, dest));
      copyMetaService.insertFileDiff(fileDiff);
    }
  }

  private class ScheduleTask implements Runnable {

    private void runningStatusUpdate() throws MetaServiceException {
      // Status update
      for (Iterator<Map.Entry<Long, Long>> it = runningDR.entrySet().iterator(); it.hasNext();) {
        Map.Entry<Long, Long> entry = it.next();
        // Check if this cmdlet is finished
        if (cmdletMetaService.getCmdletById(entry.getKey()).getState() == CmdletState.DONE) {
          // Remove from running list
          copyMetaService.markFileDiffApplied(entry.getValue());
          it.remove();
        }
      }
    }

    private void enQueue() throws IOException, ParseException {
      // Move diffs to running queue
      while (runningDR.size() < MAX_RUNNING_SIZE) {
        FileDiff fileDiff = pendingDR.poll();
        LOG.info("filediff {}", fileDiff.getParameters());
        String cmd = cmdParsing(fileDiff, srcBase, destBase);
        CmdletDescriptor cmdletDescriptor = CmdletDescriptor.fromCmdletString(cmd);
        LOG.info("cmd = {}", cmd);
        long cid = cmdletManager.submitCmdlet(cmdletDescriptor);
        runningDR.put(cid, fileDiff.getDiffId());
      }
    }

    private void addToPending() throws MetaServiceException {
      List<FileDiff> latestFileDiff = copyMetaService.getLatestFileDiff();
      for (FileDiff fileDiff : latestFileDiff) {
        // TODO filter with src and dest
        if (!pendingDR.contains(fileDiff) && fileDiff.getParameters().contains(srcBase)) {
          pendingDR.add(fileDiff);
        }
      }
    }

    @Override
    public void run() {
      try {
        // Add new diffs to pending list
        addToPending();
        runningStatusUpdate();
        enQueue();
      } catch (IOException | MetaServiceException | ParseException e) {
        LOG.error("Disaster Recovery Manager schedule error", e);
      }
    }
  }

}
