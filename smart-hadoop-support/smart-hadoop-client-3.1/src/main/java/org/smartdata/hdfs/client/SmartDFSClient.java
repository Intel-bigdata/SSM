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

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.SmartInputStreamFactory;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsPathHandle;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.client.SmartClient;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.FileState;
import org.smartdata.model.NormalFileState;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class SmartDFSClient extends DFSClient {
  private static final Logger LOG = LoggerFactory.getLogger(SmartDFSClient.class);
  private static final String CALLER_CLASS = "org.apache.hadoop.hdfs.DFSClient";
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
      InetSocketAddress[] smartServerAddress) throws IOException {
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
    return open(src, 4096, true);
  }

  /**
   * Functionality: create an InputStream and report access event to SSM server.
   *
   * DFSClient is firstly used to get an Inpustream to get file length. The real
   * InputStream returned is obtained from SmartInputStreamFactory which has some
   * considerations about compression, compact, S3 etc.
   *
   * It is supported that DFSStripedInputstream can be obtained for reading normal
   * EC data, but it is NOT supported to combine EC with SSM compact, SSM compression
   * etc. Also, there may lack of consideration on ErasureCoding in some places where
   * DFSInputStream is used instead of DFSStripedInputStream, which can lead to block
   * not found exception.
   */
  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum) throws IOException {
    DFSInputStream is;
    FileState fileState = getFileState(src);
    if (fileState.getFileStage().equals(FileState.FileStage.PROCESSING)) {
      throw new IOException("Cannot open " + src + " when it is under PROCESSING to "
          + fileState.getFileType());
    }
    is = SmartInputStreamFactory.create(this, src,
        verifyChecksum, fileState);
    // Report access event to smart server.
    reportFileAccessEvent(src);
    return is;
  }

  @Deprecated
  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum, FileSystem.Statistics stats)
      throws IOException {
    return open(src, buffersize, verifyChecksum);
  }

  @Override
  public DFSInputStream open(HdfsPathHandle fd, int buffersize, boolean verifyChecksum) throws IOException {
    String src = fd.getPath();
    DFSInputStream is = super.open(fd, buffersize, verifyChecksum);
    if (is.getFileLength() == 0) {
      is.close();
      FileState fileState = getFileState(src);
      if (fileState.getFileStage().equals(FileState.FileStage.PROCESSING)) {
        throw new IOException("Cannot open " + src + " when it is under PROCESSING to "
            + fileState.getFileType());
      }
      is = SmartInputStreamFactory.create(this, src,
          verifyChecksum, fileState);
    }
    reportFileAccessEvent(src);
    return is;
  }

  @Override
  public boolean truncate(String src, long newLength) throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompressionFileState) {
      throw new IOException(getExceptionMsg("Append", "Compressed File"));
    }
    return super.truncate(src, newLength);
  }

  @Override
  public HdfsDataOutputStream append(final String src, final int buffersize,
      EnumSet<CreateFlag> flag, final Progressable progress,
      final FileSystem.Statistics statistics) throws IOException {
    HdfsDataOutputStream out = super.append(src, buffersize, flag, progress, statistics);
    if (out.getPos() == 0) {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompactFileState) {
        out.close();
        throw new IOException(getExceptionMsg("Append", "SSM Small File"));
      }
    } else {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompressionFileState) {
        out.close();
        throw new IOException(getExceptionMsg("Append", "Compressed File"));
      }
    }
    return out;
  }

  @Override
  public HdfsDataOutputStream append(final String src, final int buffersize,
      EnumSet<CreateFlag> flag, final Progressable progress,
      final FileSystem.Statistics statistics,
      final InetSocketAddress[] favoredNodes) throws IOException {
    HdfsDataOutputStream out = super.append(
        src, buffersize, flag, progress, statistics, favoredNodes);
    if (out.getPos() == 0) {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompactFileState) {
        out.close();
        throw new IOException(getExceptionMsg("Append", "SSM Small File"));
      }
    } else {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompressionFileState) {
        out.close();
        throw new IOException(getExceptionMsg("Append", "Compressed File"));
      }
    }
    return out;
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    HdfsFileStatus oldStatus = super.getFileInfo(src);
    if (oldStatus == null) return null;
    if (oldStatus.getLen() == 0) {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompactFileState) {
        long len = ((CompactFileState) fileState).getFileContainerInfo().getLength();
        return CompatibilityHelperLoader.getHelper().createHdfsFileStatus(len, oldStatus.isDir(), oldStatus.getReplication(),
            oldStatus.getBlockSize(), oldStatus.getModificationTime(), oldStatus.getAccessTime(),
            oldStatus.getPermission(), oldStatus.getOwner(), oldStatus.getGroup(),
            oldStatus.isSymlink() ? oldStatus.getSymlinkInBytes() : null,
            oldStatus.isEmptyLocalName() ? new byte[0] : oldStatus.getLocalNameInBytes(),
            oldStatus.getFileId(), oldStatus.getChildrenNum(),
            oldStatus.getFileEncryptionInfo(), oldStatus.getStoragePolicy());
      }
    } else {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompressionFileState) {
        // To make SmartDFSClient return the original length of compressed file.
        long len = ((CompressionFileState) fileState).getOriginalLength();
        return CompatibilityHelperLoader.getHelper().createHdfsFileStatus(len, oldStatus.isDir(), oldStatus.getReplication(),
            oldStatus.getBlockSize(), oldStatus.getModificationTime(), oldStatus.getAccessTime(),
            oldStatus.getPermission(), oldStatus.getOwner(), oldStatus.getGroup(),
            oldStatus.isSymlink() ? oldStatus.getSymlinkInBytes() : null,
            oldStatus.isEmptyLocalName() ? new byte[0] : oldStatus.getLocalNameInBytes(),
            oldStatus.getFileId(), oldStatus.getChildrenNum(),
            oldStatus.getFileEncryptionInfo(), oldStatus.getStoragePolicy());
      }
    }
    return oldStatus;
  }

  @Override
  public LocatedBlocks getLocatedBlocks(String src, long start)
      throws IOException {
    LocatedBlocks locatedBlocks = super.getLocatedBlocks(src, start);
    if (!CALLER_CLASS.equals(Thread.currentThread().getStackTrace()[2].getClassName())
        && locatedBlocks.getFileLength() == 0) {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompactFileState) {
        String containerFile = ((CompactFileState) fileState)
            .getFileContainerInfo().getContainerFilePath();
        long offset = ((CompactFileState) fileState).getFileContainerInfo().getOffset();
        return super.getLocatedBlocks(containerFile, offset + start);
      }
    }
    return locatedBlocks;
  }

  @Override
  public BlockLocation[] getBlockLocations(String src, long start,
      long length) throws IOException {
    BlockLocation[] blockLocations = super.getBlockLocations(src, start, length);
    if (blockLocations.length == 0) {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompactFileState) {
        String containerFile = ((CompactFileState) fileState)
            .getFileContainerInfo().getContainerFilePath();
        long offset = ((CompactFileState) fileState).getFileContainerInfo().getOffset();
        blockLocations = super.getBlockLocations(containerFile, offset + start, length);
        for (BlockLocation blockLocation : blockLocations) {
          blockLocation.setOffset(blockLocation.getOffset() - offset);
        }
        return blockLocations;
      }
    } else {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompressionFileState) {
        CompressionFileState compressionInfo = (CompressionFileState) fileState;
        Long[] originalPos =
            compressionInfo.getOriginalPos().clone();
        Long[] compressedPos =
            compressionInfo.getCompressedPos().clone();
        int startIndex = compressionInfo.getPosIndexByOriginalOffset(start);
        int endIndex =
            compressionInfo.getPosIndexByOriginalOffset(start + length - 1);
        long compressedStart = compressedPos[startIndex];
        long compressedLength = 0;
        if (endIndex < compressedPos.length - 1) {
          compressedLength = compressedPos[endIndex + 1] - compressedStart;
        } else {
          compressedLength =
              compressionInfo.getCompressedLength() - compressedStart;
        }

        LocatedBlocks originalLocatedBlocks =
            super.getLocatedBlocks(src, compressedStart, compressedLength);

        List<LocatedBlock> blocks = new ArrayList<>();
        for (LocatedBlock block : originalLocatedBlocks.getLocatedBlocks()) {
          // TODO handle CDH2.6 storage type
          // blocks.add(new LocatedBlock(
          //     block.getBlock(),
          //     block.getLocations(),
          //     block.getStorageIDs(),
          //     block.getStorageTypes(),
          //     compressionInfo
          //         .getPosIndexByCompressedOffset(block.getStartOffset()),
          //     block.isCorrupt(),
          //     block.getCachedLocations()
          // ));
          blocks.add(new LocatedBlock(
              block.getBlock(),
              block.getLocations(),
              block.getStorageIDs(),
              block.getStorageTypes(),
              compressionInfo
                  .getPosIndexByCompressedOffset(block.getStartOffset()),
              block.isCorrupt(),
              block.getCachedLocations()
          ));
        }
        LocatedBlock lastLocatedBlock =
            originalLocatedBlocks.getLastLocatedBlock();
        long fileLength = compressionInfo.getOriginalLength();

        return new LocatedBlocks(fileLength,
            originalLocatedBlocks.isUnderConstruction(),
            blocks,
            lastLocatedBlock,
            originalLocatedBlocks.isLastBlockComplete(),
            originalLocatedBlocks.getFileEncryptionInfo(),
            originalLocatedBlocks.getErasureCodingPolicy())
            .getLocatedBlocks().toArray(new BlockLocation[0]);
      }
    }
    return blockLocations;
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Set replication", "SSM Small File"));
    } else {
      return super.setReplication(src, replication);
    }
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Set storage policy", "SSM Small File"));
    } else {
      super.setStoragePolicy(src, policyName);
    }
  }

  @Override
  public long getBlockSize(String f) throws IOException {
    long blockSize = super.getBlockSize(f);
    FileState fileState = getFileState(f);
    if (fileState instanceof CompactFileState) {
      blockSize = super.getBlockSize(((CompactFileState) fileState)
          .getFileContainerInfo().getContainerFilePath());
    }
    return blockSize;
  }

  @Override
  public void concat(String trg, String [] srcs) throws IOException {
    try {
      super.concat(trg, srcs);
    } catch (IOException e) {
      for (String src : srcs) {
        FileState fileState = getFileState(src);
        if (fileState instanceof CompactFileState) {
          throw new IOException(getExceptionMsg("Concat", "SSM Small File"));
        } else if (fileState instanceof CompressionFileState) {
          throw new IOException(getExceptionMsg("Concat", "Compressed File"));
        }
      }
      throw e;
    }
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    HdfsFileStatus fileStatus = super.getFileLinkInfo(src);
    if (fileStatus.getLen() == 0) {
      String target = super.getLinkTarget(src);
      FileState fileState = getFileState(target);
      if (fileState instanceof CompactFileState) {
        fileStatus = getFileInfo(target);
      }
    }
    return fileStatus;
  }

  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(String src, long length)
      throws IOException {
    MD5MD5CRC32FileChecksum ret = super.getFileChecksum(src, length);
    if (ret.getChecksumOpt().getBytesPerChecksum() == 0) {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompactFileState) {
//        throw new IOException(getExceptionMsg("Get file checksum", "SSM Small File"));
        return new MD5MD5CRC32FileChecksum();
      }
    }
    return ret;
  }

  @Override
  public void setPermission(String src, FsPermission permission)
      throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Set permission", "SSM Small File"));
    } else {
      super.setPermission(src, permission);
    }
  }

  @Override
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Set owner", "SSM Small File"));
    } else {
      super.setOwner(src, username, groupname);
    }
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    CorruptFileBlocks corruptFileBlocks = super.listCorruptFileBlocks(path, cookie);
    FileState fileState = getFileState(path);
    if (fileState instanceof CompactFileState) {
      corruptFileBlocks = super.listCorruptFileBlocks(((CompactFileState) fileState)
          .getFileContainerInfo().getContainerFilePath(), cookie);
    }
    return corruptFileBlocks;
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Modify acl entries", "SSM Small File"));
    } else {
      super.modifyAclEntries(src, aclSpec);
    }
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec)
      throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Remove acl entries", "SSM Small File"));
    } else {
      super.removeAclEntries(src, aclSpec);
    }
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Remove default acl", "SSM Small File"));
    } else {
      super.removeDefaultAcl(src);
    }
  }

  @Override
  public void removeAcl(String src) throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Remove acl", "SSM Small File"));
    } else {
      super.removeAcl(src);
    }
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Set acl", "SSM Small File"));
    } else {
      super.setAcl(src, aclSpec);
    }
  }

  @Override
  public void createEncryptionZone(String src, String keyName)
      throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      throw new IOException(getExceptionMsg("Create encryption zone", "SSM Small File"));
    } else {
      super.createEncryptionZone(src, keyName);
    }
  }

  @Override
  public void checkAccess(String src, FsAction mode) throws IOException {
    FileState fileState = getFileState(src);
    if (fileState instanceof CompactFileState) {
      super.checkAccess(((CompactFileState) fileState)
          .getFileContainerInfo().getContainerFilePath(), mode);
    } else {
      super.checkAccess(src, mode);
    }
  }

  @Override
  public boolean isFileClosed(String src) throws IOException {
    boolean isFileClosed = super.isFileClosed(src);
    if (!isFileClosed) {
      FileState fileState = getFileState(src);
      if (fileState instanceof CompactFileState) {
        String containerFile = ((CompactFileState) fileState)
            .getFileContainerInfo().getContainerFilePath();
        isFileClosed = super.isFileClosed(containerFile);
      }
    }
    return isFileClosed;
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      super.close();
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

  /**
   * Report file access event to SSM server.
   */
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

  /**
   * Check if the smart client is disabled.
   */
  private boolean isSmartClientDisabled() {
    File idFile = new File(SmartConstants.SMART_CLIENT_DISABLED_ID_FILE);
    return idFile.exists();
  }

  /**
   * Get the exception message of unsupported operation.
   *
   * @param operation the hdfs operation name
   * @param fileType the type of SSM specify file
   * @return the message of unsupported exception
   */
  public String getExceptionMsg(String operation, String fileType) {
    return String.format("%s is not supported for %s.", operation, fileType);
  }

  /**
   * Get file state of the specified file.
   *
   * @param filePath the path of source file
   * @return file state of source file
   * @throws IOException e
   */
  public FileState getFileState(String filePath) throws IOException {
    try {
      byte[] fileState = getXAttr(filePath, SmartConstants.SMART_FILE_STATE_XATTR_NAME);
      if (fileState != null) {
        return (FileState) SerializationUtils.deserialize(fileState);
      }
    } catch (RemoteException e) {
      return new NormalFileState(filePath);
    }

    return new NormalFileState(filePath);
  }
}
