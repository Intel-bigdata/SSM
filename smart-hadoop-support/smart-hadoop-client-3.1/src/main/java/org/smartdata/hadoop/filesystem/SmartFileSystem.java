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
package org.smartdata.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.util.Progressable;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.CompressionTrunk;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileState;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

/**
 * SmartFileSystem Deploy Guide
 * 1. Build SSM, get all jar files start with name Smart*
 * 2. Copy these jar files to HDFS classpath
 * 3. Reconfigure HDFS
 *    Please do the following configurations,
 *    1. core-site.xml
 *    Change property "fs.hdfs.impl" value, to point to the Smart Server provided
 *    "Smart File System".
 *    <property>
 *      <name>fs.hdfs.impl</name>
 *      <value>org.smartdata.hadoop.filesystem.SmartFileSystem</value>
 *      <description>The FileSystem for hdfs URL</description>
 *    </property>
 *    2. hdfs-site.xml
 *    Add property "smart.server.rpc.address" to point to Smart Server.
 *    <property>
 *      <name>smart.server.rpc.address</name>
 *      <value>127.0.0.1:7042</value>
 *    </property>
 *
 * 4. Restart HDFS
 */
public class SmartFileSystem extends DistributedFileSystem {
  private SmartDFSClient smartDFSClient;
  private boolean verifyChecksum = true;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    String[] rpcConfValue =
        conf.getTrimmedStrings(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    if (rpcConfValue == null) {
      throw new IOException("SmartServer address not found. Please configure "
          + "it through " + SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    }

    List<InetSocketAddress> addrList = new LinkedList<>();
    for (String rpcValue : rpcConfValue) {
      String[] hostAndPort = rpcValue.split(":");
      try {
        InetSocketAddress smartServerAddress = new InetSocketAddress(
            hostAndPort[hostAndPort.length - 2],
            Integer.parseInt(hostAndPort[hostAndPort.length - 1]));
        addrList.add(smartServerAddress);
      } catch (Exception e) {
        throw new IOException("Incorrect SmartServer address. Please follow the "
            + "IP/Hostname:Port format");
      }
    }
    this.smartDFSClient = new SmartDFSClient(conf,
        (InetSocketAddress[]) addrList.toArray());
  }

  @Override
  public FSDataInputStream open(Path path, final int bufferSize)
    throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(path);
    final DFSInputStream in = smartDFSClient.open(
        absF.toUri().getPath(), bufferSize, verifyChecksum);
    return smartDFSClient.createWrappedInputStream(in);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  public FSDataOutputStream append(Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    return append(f, EnumSet.of(CreateFlag.APPEND), bufferSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress)
      throws IOException {
    FSDataOutputStream out = super.append(f, flag, bufferSize, progress);
    if (out.getPos() == 0) {
      FileState fileState = smartDFSClient.getFileState(getPathName(f));
      if (fileState instanceof CompactFileState) {
        throw new IOException(
            smartDFSClient.getExceptionMsg("Append", "SSM Small File"));
      }
    } else {
      FileState fileState = smartDFSClient.getFileState(getPathName(f));
      if (fileState instanceof CompressionFileState) {
        throw new IOException(
            smartDFSClient.getExceptionMsg("Append", "Compressed File"));
      }
    }
    return out;
  }

  @Override
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
      final int bufferSize, final Progressable progress,
      final InetSocketAddress[] favoredNodes)
      throws IOException {
    FSDataOutputStream out = super.append(f, flag, bufferSize, progress, favoredNodes);
    if (out.getPos() == 0) {
      FileState fileState = smartDFSClient.getFileState(getPathName(f));
      if (fileState instanceof CompactFileState) {
        throw new IOException(
            smartDFSClient.getExceptionMsg("Append", "SSM Small File"));
      }
    } else {
      FileState fileState = smartDFSClient.getFileState(getPathName(f));
      if (fileState instanceof CompressionFileState) {
        throw new IOException(
            smartDFSClient.getExceptionMsg("Append", "Compressed File"));
      }
    }
    return out;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus oldStatus = super.getFileStatus(f);
    if (oldStatus == null) return null;
    if (oldStatus.getLen() == 0) {
      FileState fileState = smartDFSClient.getFileState(getPathName(f));
      if (fileState instanceof CompactFileState) {
        long len = ((CompactFileState) fileState).getFileContainerInfo().getLength();
        return new FileStatus(len, oldStatus.isDirectory(), oldStatus.getReplication(),
            oldStatus.getBlockSize(), oldStatus.getModificationTime(),
            oldStatus.getAccessTime(), oldStatus.getPermission(),
            oldStatus.getOwner(), oldStatus.getGroup(),
            oldStatus.isSymlink() ? oldStatus.getSymlink() : null, oldStatus.getPath());
      }
    } else {
      FileState fileState = smartDFSClient.getFileState(getPathName(f));
      if (fileState instanceof CompressionFileState) {
        long len = ((CompressionFileState) fileState).getOriginalLength();
        return new FileStatus(len, oldStatus.isDirectory(), oldStatus.getReplication(),
            oldStatus.getBlockSize(), oldStatus.getModificationTime(),
            oldStatus.getAccessTime(), oldStatus.getPermission(),
            oldStatus.getOwner(), oldStatus.getGroup(),
            oldStatus.isSymlink() ? oldStatus.getSymlink() : null, oldStatus.getPath());
      }
    }
    return oldStatus;
  }

  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    FileStatus[] oldStatus = super.listStatus(p);
    ArrayList<FileStatus> newStatus = new ArrayList<>(oldStatus.length);
    for (FileStatus status : oldStatus) {
      if (oldStatus == null) {
        newStatus.add(null);
        continue;
      }
      if (status.getLen() == 0) {
        FileState fileState = smartDFSClient.getFileState(getPathName(status.getPath()));
        if (fileState instanceof CompactFileState) {
          long len = ((CompactFileState) fileState).getFileContainerInfo().getLength();
          newStatus.add(new FileStatus(len, status.isDirectory(), status.getReplication(),
              status.getBlockSize(), status.getModificationTime(), status.getAccessTime(),
              status.getPermission(), status.getOwner(), status.getGroup(),
              status.isSymlink() ? status.getSymlink() : null, status.getPath()));
        } else {
          newStatus.add(status);
        }
      } else {
        FileState fileState = smartDFSClient.getFileState(getPathName(status.getPath()));
        if (fileState instanceof CompressionFileState) {
          long len = ((CompressionFileState) fileState).getOriginalLength();
          newStatus.add(new FileStatus(len, status.isDirectory(), status.getReplication(),
              status.getBlockSize(), status.getModificationTime(), status.getAccessTime(),
              status.getPermission(), status.getOwner(), status.getGroup(),
              status.isSymlink() ? status.getSymlink() : null, status.getPath()));
        } else {
          newStatus.add(status);
        }
      }
    }
    return newStatus.toArray(new FileStatus[oldStatus.length]);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, final long start,
      final long len) throws IOException {
    BlockLocation[] blockLocations = super.getFileBlockLocations(
        p, start, len);
    if (blockLocations.length == 0) {
      FileState fileState = smartDFSClient.getFileState(getPathName(p));
      if (fileState instanceof CompactFileState) {
        FileContainerInfo fileContainerInfo = ((CompactFileState) fileState).getFileContainerInfo();
        String containerFile = fileContainerInfo.getContainerFilePath();
        long offset = fileContainerInfo.getOffset();
        blockLocations = super.getFileBlockLocations(
            new Path(containerFile), offset + start, len);
        for (BlockLocation blockLocation : blockLocations) {
          blockLocation.setOffset(blockLocation.getOffset() - offset);
        }
      }
    }
    return blockLocations;
  }

  @Override
  public boolean setReplication(Path src,
      final short replication) throws IOException {
    FileState fileState = smartDFSClient.getFileState(getPathName(src));
    if (fileState instanceof CompactFileState) {
      throw new IOException(
          smartDFSClient.getExceptionMsg("Set replication", "SSM Small File"));
    } else {
      return super.setReplication(src, replication);
    }
  }

  @Override
  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    FileState fileState = smartDFSClient.getFileState(getPathName(src));
    if (fileState instanceof CompactFileState) {
      throw new IOException(
          smartDFSClient.getExceptionMsg("Set storage policy", "SSM Small File"));
    } else {
      super.setStoragePolicy(src, policyName);
    }
  }

  @Override
  public void concat(Path trg, Path [] psrcs) throws IOException {
    try {
      super.concat(trg, psrcs);
    } catch (IOException e) {
      for (Path src : psrcs) {
        FileState fileState = smartDFSClient.getFileState(getPathName(src));
        if (fileState instanceof CompactFileState) {
          throw new IOException(
              smartDFSClient.getExceptionMsg("Concat", "SSM Small File"));
        }
      }
    }
  }


  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    FileStatus fileStatus = super.getFileLinkStatus(f);
    if (fileStatus.getLen() == 0) {
      Path target = getLinkTarget(f);
      FileState fileState = smartDFSClient.getFileState(getPathName(target));
      if (fileState instanceof CompactFileState) {
        fileStatus = getFileStatus(target);
      }
    } else {
      Path target = getLinkTarget(f);
      FileState fileState = smartDFSClient.getFileState(getPathName(target));
      if (fileState instanceof CompressionFileState) {
        fileStatus = getFileStatus(target);
      }
    }
    return fileStatus;
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FileChecksum>() {
      @Override
      public FileChecksum doCall(final Path p)
          throws IOException {
        return smartDFSClient.getFileChecksum(getPathName(p), Long.MAX_VALUE);
      }

      @Override
      public FileChecksum next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.getFileChecksum(p);
      }
    }.resolve(this, absF);
  }

  @Override
  public FileChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<FileChecksum>() {
      @Override
      public FileChecksum doCall(final Path p)
          throws IOException {
        return smartDFSClient.getFileChecksum(getPathName(p), length);
      }

      @Override
      public FileChecksum next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof SmartFileSystem) {
          return fs.getFileChecksum(p, length);
        } else {
          throw new UnsupportedFileSystemException(
              "getFileChecksum(Path, long) is not supported by "
                  + fs.getClass().getSimpleName());
        }
      }
    }.resolve(this, absF);
  }

  @Override
  public void setPermission(Path p, final FsPermission permission
  ) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        smartDFSClient.setPermission(getPathName(p), permission);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setPermission(p, permission);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void setOwner(Path p, final String username, final String groupname
  ) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(p);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        smartDFSClient.setOwner(getPathName(p), username, groupname);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setOwner(p, username, groupname);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
      throws IOException {
    RemoteIterator<Path> corruptFileBlocksIterator = super.listCorruptFileBlocks(path);
    FileState fileState = smartDFSClient.getFileState(getPathName(path));
    if (fileState instanceof CompactFileState) {
      corruptFileBlocksIterator = super.listCorruptFileBlocks(
          new Path(((CompactFileState) fileState)
              .getFileContainerInfo().getContainerFilePath()));
    }
    return corruptFileBlocksIterator;
  }

  @Override
  public void modifyAclEntries(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        smartDFSClient.modifyAclEntries(getPathName(p), aclSpec);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.modifyAclEntries(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void removeAclEntries(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        smartDFSClient.removeAclEntries(getPathName(p), aclSpec);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.removeAclEntries(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    final Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        smartDFSClient.removeDefaultAcl(getPathName(p));
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException, UnresolvedLinkException {
        fs.removeDefaultAcl(p);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    final Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        smartDFSClient.removeAcl(getPathName(p));
        return null;
      }
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException, UnresolvedLinkException {
        fs.removeAcl(p);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void setAcl(Path path, final List<AclEntry> aclSpec)
      throws IOException {
    Path absF = fixRelativePart(path);
    new FileSystemLinkResolver<Void>() {
      @Override
      public Void doCall(final Path p) throws IOException {
        smartDFSClient.setAcl(getPathName(p), aclSpec);
        return null;
      }

      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setAcl(p, aclSpec);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public void createEncryptionZone(Path path, String keyName)
      throws IOException {
    smartDFSClient.createEncryptionZone(getPathName(path), keyName);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
      throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<RemoteIterator<FileStatus>>() {
      @Override
      public RemoteIterator<FileStatus> doCall(final Path p)
          throws IOException {
        return new SmartDirListingIterator<>(p, false);
      }

      @Override
      public RemoteIterator<FileStatus> next(final FileSystem fs, final Path p)
          throws IOException {
        return ((DistributedFileSystem) fs).listStatusIterator(p);
      }
    }.resolve(this, absF);
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter) throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<RemoteIterator<LocatedFileStatus>>() {
      @Override
      public RemoteIterator<LocatedFileStatus> doCall(final Path p)
          throws IOException {
        return new SmartDirListingIterator<>(p, filter, true);
      }

      @Override
      public RemoteIterator<LocatedFileStatus> next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof SmartFileSystem) {
          return ((SmartFileSystem)fs).listLocatedStatus(p, filter);
        }
        // symlink resolution for this methods does not work cross file systems
        // because it is a protected method.
        throw new IOException("Link resolution does not work with multiple " +
            "file systems for listLocatedStatus(): " + p);
      }
    }.resolve(this, absF);
  }

  private class SmartDirListingIterator<T extends FileStatus>
      implements RemoteIterator<T> {

    private DirectoryListing thisListing;
    private int i;
    private Path p;
    private String src;
    private T curStat = null;
    private PathFilter filter;
    private boolean needLocation;

    private SmartDirListingIterator(Path p, PathFilter filter,
        boolean needLocation) throws IOException {
      this.p = p;
      this.src = getPathName(p);
      this.filter = filter;
      this.needLocation = needLocation;
      // fetch the first batch of entries in the directory
      thisListing = smartDFSClient.listPaths(src, HdfsFileStatus.EMPTY_NAME,
          needLocation);
      statistics.incrementReadOps(1);
      // the directory does not exist
      if (thisListing == null) {
        throw new FileNotFoundException("File " + p + " does not exist.");
      }
      i = 0;
    }

    private SmartDirListingIterator(Path p, boolean needLocation)
        throws IOException {
      this(p, null, needLocation);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() throws IOException {
      while (curStat == null && hasNextNoFilter()) {
        T next;
        HdfsFileStatus fileStat = thisListing.getPartialListing()[i++];
        if (needLocation) {
          next = (T)((HdfsLocatedFileStatus) fileStat).makeQualifiedLocated(getUri(), p);
          String fileName = next.getPath().toUri().getPath();

          // Reconstruct FileStatus
          if (next.getLen() == 0) {
            FileState fileState = smartDFSClient.getFileState(fileName);
            if (fileState instanceof CompactFileState) {
              CompactFileState compactFileState = (CompactFileState) fileState;
              long len = compactFileState.getFileContainerInfo().getLength();
              BlockLocation[] blockLocations = smartDFSClient.getBlockLocations(
                  fileName, 0, len);
              next = (T) new LocatedFileStatus(len,
                  next.isDirectory(),
                  next.getReplication(),
                  next.getBlockSize(),
                  next.getModificationTime(),
                  next.getAccessTime(),
                  next.getPermission(),
                  next.getOwner(),
                  next.getGroup(),
                  next.isSymlink() ? next.getSymlink() : null,
                  next.getPath(),
                  blockLocations);
            }
          } else {
            FileState fileState = smartDFSClient.getFileState(fileName);
            if (fileState instanceof CompressionFileState) {
              CompressionFileState compressionFileState = (CompressionFileState) fileState;
              long fileLen = compressionFileState.getOriginalLength();
              BlockLocation[] blockLocations =
                  ((LocatedFileStatus)next).getBlockLocations();
              for (BlockLocation blockLocation : blockLocations) {
                convertBlockLocation(blockLocation, compressionFileState);
              }
              next = (T) new LocatedFileStatus(fileLen,
                  next.isDirectory(),
                  next.getReplication(),
                  next.getBlockSize(),
                  next.getModificationTime(),
                  next.getAccessTime(),
                  next.getPermission(),
                  next.getOwner(),
                  next.getGroup(),
                  next.isSymlink() ? next.getSymlink() : null,
                  next.getPath(),
                  blockLocations);
            }
          }
        } else {
          next = (T) fileStat.makeQualified(getUri(), p);
          String fileName = next.getPath().toUri().getPath();

          // Reconstruct FileStatus
          if (next.getLen() == 0) {
            FileState fileState = smartDFSClient.getFileState(fileName);
            if (fileState instanceof CompactFileState) {
              CompactFileState compactFileState = (CompactFileState) fileState;
              long len = compactFileState.getFileContainerInfo().getLength();
              next = (T) new FileStatus(len,
                  next.isDirectory(),
                  next.getReplication(),
                  next.getBlockSize(),
                  next.getModificationTime(),
                  next.getAccessTime(),
                  next.getPermission(),
                  next.getOwner(),
                  next.getGroup(),
                  next.isSymlink() ? next.getSymlink() : null,
                  next.getPath());
            }
          } else {
            FileState fileState = smartDFSClient.getFileState(fileName);
            if (fileState instanceof CompressionFileState) {
              CompressionFileState compressionFileState = (CompressionFileState) fileState;
              long fileLen = compressionFileState.getOriginalLength();
              BlockLocation[] blockLocations =
                  ((LocatedFileStatus)next).getBlockLocations();
              for (BlockLocation blockLocation : blockLocations) {
                convertBlockLocation(blockLocation, compressionFileState);
              }
              next = (T) new LocatedFileStatus(fileLen,
                  next.isDirectory(),
                  next.getReplication(),
                  next.getBlockSize(),
                  next.getModificationTime(),
                  next.getAccessTime(),
                  next.getPermission(),
                  next.getOwner(),
                  next.getGroup(),
                  next.isSymlink() ? next.getSymlink() : null,
                  next.getPath(),
                  blockLocations);
            }
          }
        }

        // apply filter if not null
        if (filter == null || filter.accept(next.getPath())) {
          curStat = next;
        }
      }
      return curStat != null;
    }

    // Definitions:
    // * Compression trunk doesn't cross over two blocks:
    // - Offset = original start of the first trunk
    // - End = original end of the last trunk
    // * Compression trunk crosses over two blocks:
    // - Offset = original middle of the first incomplete trunk
    // - End = original middle of the last incomplete trunk
    private void convertBlockLocation(BlockLocation blockLocation,
        CompressionFileState compressionInfo) throws IOException {
      long compressedStart = blockLocation.getOffset();
      long compressedEnd = compressedStart + blockLocation.getLength() - 1;

      CompressionTrunk startTrunk = compressionInfo.locateCompressionTrunk(
          true, compressedStart);
      CompressionTrunk endTrunk = compressionInfo.locateCompressionTrunk(
          true, compressedEnd);

      long originStart;
      // If the first trunk crosses over two blocks, set start as middle of the trunk
      if (startTrunk.getCompressedOffset() < compressedStart) {
        originStart = startTrunk.getOriginOffset() + startTrunk.getOriginLength() / 2 + 1;
      } else {
        originStart = startTrunk.getOriginOffset();
      }

      long originEnd;
      // If the last trunk corsses over two blocks, set end as middle of the trunk
      if (endTrunk.getCompressedOffset() + endTrunk.getCompressedLength() - 1 > compressedEnd) {
        originEnd = endTrunk.getOriginOffset() + endTrunk.getOriginLength() / 2;
      } else {
        originEnd = endTrunk.getOriginOffset() + endTrunk.getOriginLength() - 1;
      }
      blockLocation.setOffset(originStart);
      blockLocation.setLength(originEnd - originStart + 1);
    }

    /**
     * Check if there is a next item before applying the given filter
     */
    private boolean hasNextNoFilter() throws IOException {
      if (thisListing == null) {
        return false;
      }
      if (i >= thisListing.getPartialListing().length && thisListing.hasMore()) {
        // current listing is exhausted & fetch a new listing
        thisListing = smartDFSClient.listPaths(
            src, thisListing.getLastName(), needLocation);
        statistics.incrementReadOps(1);
        if (thisListing == null) {
          return false;
        }
        i = 0;
      }
      return (i < thisListing.getPartialListing().length);
    }

    @Override
    public T next() throws IOException {
      if (hasNext()) {
        T tmp = curStat;
        curStat = null;
        return tmp;
      }
      throw new java.util.NoSuchElementException("No more entry in " + p);
    }
  }

  @Override
  public boolean isFileClosed(final Path src) throws IOException {
    boolean isFileClosed = super.isFileClosed(src);
    if (!isFileClosed) {
      FileState fileState = smartDFSClient.getFileState(getPathName(src));
      if (fileState instanceof CompactFileState) {
        String containerFile = ((CompactFileState) fileState)
            .getFileContainerInfo().getContainerFilePath();
        isFileClosed = smartDFSClient.isFileClosed(containerFile);
      }
    }
    return isFileClosed;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (smartDFSClient != null) {
        this.smartDFSClient.close();
      }
    }
  }

  /**
   * Checks that the passed URI belongs to this filesystem and returns
   * just the path component. Expects a URI with an absolute path.
   *
   * @param file URI with absolute path
   * @return path component of {file}
   * @throws IllegalArgumentException if URI does not belong to this DFS
   */
  private String getPathName(Path file) {
    checkPath(file);
    String result = fixRelativePart(file).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
          file + " is not a valid DFS filename.");
    }
    return result;
  }
}
