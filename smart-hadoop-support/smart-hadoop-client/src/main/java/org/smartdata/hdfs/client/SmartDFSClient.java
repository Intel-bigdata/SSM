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
package org.smartdata.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.SmartInputStreamFactory;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.client.SmartClient;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileState;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

public class SmartDFSClient extends DFSClient {
  private static final Logger LOG = LoggerFactory.getLogger(SmartDFSClient.class);
  private SmartClient smartClient = null;
  private boolean healthy = false;

  public SmartDFSClient(InetSocketAddress nameNodeAddress, Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeAddress, conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(final URI nameNodeUri, final Configuration conf,
      final InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeUri, conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(URI nameNodeUri, Configuration conf,
      FileSystem.Statistics stats, InetSocketAddress smartServerAddress)
      throws IOException {
    super(nameNodeUri, conf, stats);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(Configuration conf) throws IOException {
    super(conf);
    if (isSmartClientDisabled()) {
      return;
    }
    try {
      smartClient = new SmartClient(conf);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  @Override
  public DFSInputStream open(String src) throws IOException {
    return open(src, 1024, true);
  }

  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum) throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState.getFileStage().equals(FileState.FileStage.PROCESSING)) {
      throw new IOException("Cannot open " + src + " when it is under PROCESSING to "
          + fileState.getFileType());
    }
    DFSInputStream is = SmartInputStreamFactory.get().create(this, src,
        verifyChecksum, fileState);
    reportFileAccessEvent(src);
    return is;
  }

  @Deprecated
  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum, FileSystem.Statistics stats)
      throws IOException {
    return super.open(src, buffersize, verifyChecksum, stats);
  }

  @Override
  public LocatedBlocks getLocatedBlocks(String src, long start)
      throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      String containerFile = ((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath();
      long offset = ((CompactFileState) fileState).getFileContainerInfo().getOffset();
      return super.getLocatedBlocks(containerFile, offset + start);
    } else {
      return super.getLocatedBlocks(src, start);
    }
  }

  @Override
  public BlockLocation[] getBlockLocations(String src, long start,
                                           long length) throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      String containerFile = ((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath();
      long offset = ((CompactFileState) fileState).getFileContainerInfo().getOffset();
      return super.getBlockLocations(containerFile, offset + start, length);
    } else {
      return super.getBlockLocations(src, start, length);
    }
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    HdfsFileStatus oldStatus = super.getFileInfo(src);
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      long len = ((CompactFileState) fileState).getFileContainerInfo().getLength();
      return new HdfsFileStatus(len, oldStatus.isDir(), oldStatus.getReplication(),
          oldStatus.getBlockSize(), oldStatus.getModificationTime(), oldStatus.getAccessTime(),
          oldStatus.getPermission(), oldStatus.getOwner(), oldStatus.getGroup(),
          oldStatus.getSymlinkInBytes(), oldStatus.getLocalNameInBytes(), oldStatus.getFileId(),
          oldStatus.getChildrenNum(), oldStatus.getFileEncryptionInfo(), oldStatus.getStoragePolicy());
    } else {
      return oldStatus;
    }
  }

  @Override
  @Deprecated
  public boolean delete(String src) throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      smartClient.deleteSmallFile(src);
      return super.delete(src);
    } else {
      return super.delete(src);
    }
  }

  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      smartClient.deleteSmallFile(src);
      return super.delete(src, recursive);
    } else {
      return super.delete(src, recursive);
    }
  }

  @Override
  public boolean truncate(String src, long newLength) throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState && newLength == 0) {
      smartClient.truncateSmallFile(src);
      return super.truncate(src, 0);
    } else {
      return super.truncate(src, newLength);
    }
  }

  @Override
  @Deprecated
  public boolean rename(String src, String dst) throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      smartClient.renameSmallFile(src, dst);
      return super.rename(src, dst);
    } else {
      return super.rename(src, dst);
    }
  }

  @Override
  public void rename(String src, String dst, Options.Rename... options)
      throws IOException {
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      smartClient.renameSmallFile(src, dst);
      super.rename(src, dst, options);
    } else {
      super.rename(src, dst, options);
    }
  }

  private void reportFileAccessEvent(String src) {
    try {
      if (!healthy) {
        return;
      }
      String userName;
      try {
        userName = UserGroupInformation.getCurrentUser().getUserName();
      } catch (IOException e) {
        userName = "Unknown";
      }
      smartClient.reportFileAccessEvent(new FileAccessEvent(src, userName));
    } catch (IOException e) {
      // Here just ignores that failed to report
      LOG.error("Cannot report file access event to SmartServer: " + src
          + " , for: " + e.getMessage()
          + " , report mechanism will be disabled now in this instance.");
      healthy = false;
    }
  }

  public FileState getFileState(String filePath) throws IOException {
    return smartClient.getFileState(filePath);
  }

  private boolean isSmartClientDisabled() {
    File idFile = new File(SmartConstants.SMART_CLIENT_DISABLED_ID_FILE);
    return idFile.exists();
  }

  @Override
  public boolean isFileClosed(String src) throws IOException{
    FileState fileState = smartClient.getFileState(src);
    if (fileState instanceof CompactFileState) {
      String containerFile = ((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath();
      return super.isFileClosed(containerFile);
    } else {
      return super.isFileClosed(src);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      super.close();
    } catch (IOException e) {
      throw e;
    } finally {
      try {
        if (smartClient != null) {
          smartClient.close();
        }
      } finally {
        healthy = false;
      }
    }
  }
}
