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
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
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
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileState;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;

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
 *    Add property "smart.server.rpc.adddress" to point to Smart Server.
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

    String rpcConfValue = conf.get(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    if (rpcConfValue == null) {
      throw new IOException("SmartServer address not found. Please configure "
          + "it through " + SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    }

    String[] strings = rpcConfValue.split(":");
    InetSocketAddress smartServerAddress;
    try {
      smartServerAddress = new InetSocketAddress(
          strings[strings.length - 2],
          Integer.parseInt(strings[strings.length - 1]));
    } catch (Exception e) {
      throw new IOException("Incorrect SmartServer address. Please follow the "
          + "IP/Hostname:Port format");
    }
    this.smartDFSClient = new SmartDFSClient(conf, smartServerAddress);
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
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus oldStatus = super.getFileStatus(f);
    FileState fileState = smartDFSClient.getFileState(getPathName(f));
    if (fileState instanceof CompactFileState) {
      long len = ((CompactFileState) fileState).getFileContainerInfo().getLength();
      return new FileStatus(len, oldStatus.isDirectory(), oldStatus.getReplication(),
          oldStatus.getBlockSize(), oldStatus.getModificationTime(), oldStatus.getAccessTime(),
          oldStatus.getPermission(), oldStatus.getOwner(), oldStatus.getGroup(),
          oldStatus.isSymlink() ? oldStatus.getSymlink() : null, oldStatus.getPath());
    } else {
      return oldStatus;
    }
  }

  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    FileStatus[] oldStatus = super.listStatus(p);
    ArrayList<FileStatus> newStatus = new ArrayList<>(oldStatus.length);
    for (FileStatus status : oldStatus) {
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
    }
    return newStatus.toArray(new FileStatus[oldStatus.length]);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, final long start,
                                               final long len) throws IOException {
    FileState fileState = smartDFSClient.getFileState(getPathName(p));
    if (fileState instanceof CompactFileState) {
      FileContainerInfo fileContainerInfo = ((CompactFileState) fileState).getFileContainerInfo();
      String containerFile = fileContainerInfo.getContainerFilePath();
      long offset = fileContainerInfo.getOffset();
      BlockLocation[] blockLocations = super.getFileBlockLocations(
          new Path(containerFile), offset + start, len);
      for (BlockLocation blockLocation : blockLocations) {
        blockLocation.setOffset(blockLocation.getOffset() - offset);
      }
      return blockLocations;
    } else {
      return super.getFileBlockLocations(p, start, len);
    }
  }

  private boolean isCompactFile(final Path p) throws IOException {
    FileState fileState = smartDFSClient.getFileState(getPathName(p));
    return fileState instanceof CompactFileState;
  }

  @Override
  public boolean recoverLease(final Path f) throws IOException {
    FileState fileState = smartDFSClient.getFileState(getPathName(f));
    if (fileState instanceof CompactFileState) {
      return super.recoverLease(
          new Path(((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath()));
    } else {
      return super.recoverLease(f);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, final int bufferSize,
                                   final Progressable progress) throws IOException {
    if (isCompactFile(f)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      return super.append(f, bufferSize, progress);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
                                   final int bufferSize, final Progressable progress)
      throws IOException {
    if (isCompactFile(f)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      return super.append(f, flag, bufferSize, progress);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag,
                                   final int bufferSize, final Progressable progress,
                                   final InetSocketAddress[] favoredNodes)
      throws IOException {
    if (isCompactFile(f)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      return super.append(f, flag, bufferSize, progress, favoredNodes);
    }
  }

  @Override
  public boolean setReplication(Path src,
                                final short replication) throws IOException {
    if (isCompactFile(src)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      return super.setReplication(src, replication);
    }
  }

  @Override
  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    if (isCompactFile(src)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      super.setStoragePolicy(src, policyName);
    }
  }

  @Override
  public void concat(Path trg, Path [] psrcs) throws IOException {
    for (Path src : psrcs) {
      if (isCompactFile(src)) {
        throw new IOException("This operation not supported for SSM small file.");
      }
    }
    super.concat(trg, psrcs);
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    statistics.incrementWriteOps(1);

    final Path absSrc = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);

    // Try the rename without resolving first
    try {
      return smartDFSClient.rename(getPathName(absSrc), getPathName(absDst));
    } catch (UnresolvedLinkException e) {
      // Fully resolve the source
      final Path source = getFileLinkStatus(absSrc).getPath();
      // Keep trying to resolve the destination
      return new FileSystemLinkResolver<Boolean>() {
        @Override
        public Boolean doCall(final Path p)
            throws IOException {
          return smartDFSClient.rename(getPathName(source), getPathName(p));
        }
        @Override
        public Boolean next(final FileSystem fs, final Path p)
            throws IOException {
          // Should just throw an error in FileSystem#checkPath
          return doCall(p);
        }
      }.resolve(this, absDst);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, final Options.Rename... options)
      throws IOException {
    statistics.incrementWriteOps(1);
    final Path absSrc = fixRelativePart(src);
    final Path absDst = fixRelativePart(dst);
    // Try the rename without resolving first
    try {
      smartDFSClient.rename(getPathName(absSrc), getPathName(absDst), options);
    } catch (UnresolvedLinkException e) {
      // Fully resolve the source
      final Path source = getFileLinkStatus(absSrc).getPath();
      // Keep trying to resolve the destination
      new FileSystemLinkResolver<Void>() {
        @Override
        public Void doCall(final Path p)
            throws IOException {
          smartDFSClient.rename(getPathName(source), getPathName(p), options);
          return null;
        }
        @Override
        public Void next(final FileSystem fs, final Path p)
            throws IOException {
          // Should just throw an error in FileSystem#checkPath
          return doCall(p);
        }
      }.resolve(this, absDst);
    }
  }

  @Override
  public boolean truncate(Path f, final long newLength) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p)
          throws IOException {
        return smartDFSClient.truncate(getPathName(p), newLength);
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.truncate(p, newLength);
      }
    }.resolve(this, absF);
  }

  @Override
  public boolean delete(Path f, final boolean recursive) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    return new FileSystemLinkResolver<Boolean>() {
      @Override
      public Boolean doCall(final Path p)
          throws IOException {
        return smartDFSClient.delete(getPathName(p), recursive);
      }
      @Override
      public Boolean next(final FileSystem fs, final Path p)
          throws IOException {
        return fs.delete(p, recursive);
      }
    }.resolve(this, absF);
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
                                                                final PathFilter filter)
      throws IOException {
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
          next = (T) fileStat.makeQualified(getUri(), p);
          String fileName = next.getPath().toUri().getPath();

          // Reconstruct FileStatus
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
        }

        // apply filter if not null
        if (filter == null || filter.accept(next.getPath())) {
          curStat = next;
        }
      }
      return curStat != null;
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
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
      throws IOException {
    FileState fileState = smartDFSClient.getFileState(getPathName(path));
    if (fileState instanceof CompactFileState) {
      return super.listCorruptFileBlocks(
          new Path(((CompactFileState) fileState)
              .getFileContainerInfo().getContainerFilePath()));
    } else {
      return super.listCorruptFileBlocks(path);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createSymlink(final Path target, final Path link,
                            final boolean createParent) throws IOException {
    if (isCompactFile(target)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      super.createSymlink(target, link, createParent);
    }
  }

  @Override
  public FileStatus getFileLinkStatus(final Path f) throws IOException {
    if (isCompactFile(f)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      return super.getFileLinkStatus(f);
    }
  }

  @Override
  public Path getLinkTarget(final Path f) throws IOException {
    if (isCompactFile(f)) {
      throw new IOException("This operation not supported for SSM small file.");
    } else {
      return super.getLinkTarget(f);
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

  @Override
  public boolean isFileClosed(final Path src) throws IOException {
    FileState fileState = smartDFSClient.getFileState(getPathName(src));
    if (fileState instanceof CompactFileState) {
      String containerFile = (
          (CompactFileState) fileState).getFileContainerInfo().getContainerFilePath();
      return super.isFileClosed(new Path(containerFile));
    } else {
      return super.isFileClosed(src);
    }
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
}
