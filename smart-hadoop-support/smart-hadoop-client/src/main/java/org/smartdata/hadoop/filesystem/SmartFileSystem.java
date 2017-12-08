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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.model.CompressionTrunk;
import org.smartdata.model.SmartFileCompressionInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

/**
 * SmartFileSystem Deploy Guide
 * 1. Build SSM, get all jar files start with name Smart*
 * 2. Copy these jar files to HDFS classpath
 * 3. Reconfigure HDFS
 *   Please do the following configurations,
 *   1. core-site.xml
 *   Change property "fs.hdfs.impl" value, to point to the Smart Server provided
 *   "Smart  File System".
 *    <property>
 *      <name>fs.hdfs.impl</name>
 *      <value>org.smartdata.hadoop.filesystem.SmartFileSystem</value>
 *      <description>The FileSystem for hdfs URL</description>
 *    </property>
 *    2. hdfs-site.xml
 *    Add property "smart.server.rpc.adddress" and "smart.server.rpc.port" to
 *    point to installed Smart Server.
 *    <property>
 *      <name>smart.server.rpc.address</name>
 *      <value>127.0.0.1</value>
 *    </property>
 *    <property>
 *      <name>smart.server.rpc.port</name>
 *      <value>7042</value>
 *    </property>
 * 4. Restart HDFS
 */

public class SmartFileSystem extends DistributedFileSystem {
  private SmartDFSClient smartClient;
  private InetSocketAddress smartServerAddress;
  private boolean verifyChecksum = true;
  private boolean healthy;

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

    this.smartClient = new SmartDFSClient(conf, smartServerAddress);
  }

  @Override
  public FSDataInputStream open(Path path, final int bufferSize)
    throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(path);
    final DFSInputStream dfsis = smartClient.open(absF.toUri().getPath(), bufferSize, verifyChecksum);
    return smartClient.createWrappedInputStream(dfsis);
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } catch (IOException e) {
      throw e;
    } finally {
      try {
        if (smartClient != null) {
          this.smartClient.close();
        }
      } finally {
        healthy = false;
      }
    }
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path p,
      final PathFilter filter) throws IOException {
    Path absF = fixRelativePart(p);
    return new FileSystemLinkResolver<RemoteIterator<LocatedFileStatus>>() {
      @Override
      public RemoteIterator<LocatedFileStatus> doCall(final Path p)
          throws IOException, UnresolvedLinkException {
        return new SmartDirListingIterator<LocatedFileStatus>(p, filter, true);
      }

      @Override
      public RemoteIterator<LocatedFileStatus> next(final FileSystem fs, final Path p)
          throws IOException {
        if (fs instanceof SmartFileSystem) {
          return ((SmartFileSystem)fs).listLocatedStatus(p, filter);
        }
        // symlink resolution for this methos does not work cross file systems
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
      thisListing = smartClient.listPaths(src, HdfsFileStatus.EMPTY_NAME,
          needLocation);
      statistics.incrementReadOps(1);
      if (thisListing == null) { // the directory does not exist
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
          next = (T)((HdfsLocatedFileStatus)fileStat)
              .makeQualifiedLocated(getUri(), p);
          String fileName = next.getPath().toUri().getPath();
          // Reconstruct FileStatus if this file is compressed
          // Two segments to be replaced : length, blockLocations
          if (smartClient.isFileCompressed(fileName)) {
            SmartFileCompressionInfo compressionInfo =
                smartClient.getFileCompressionInfo(fileName);
            long fileLen = compressionInfo.getOriginalLength();
            BlockLocation[] blockLocations =
                ((LocatedFileStatus)next).getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
              convertBlockLocation(blockLocation, compressionInfo);
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
        } else {
          next = (T)fileStat.makeQualified(getUri(), p);
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
        SmartFileCompressionInfo compressionInfo) throws IOException {
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

    /** Check if there is a next item before applying the given filter */
    private boolean hasNextNoFilter() throws IOException {
      if (thisListing == null) {
        return false;
      }
      if (i >= thisListing.getPartialListing().length
          && thisListing.hasMore()) {
        // current listing is exhausted & fetch a new listing
        thisListing = smartClient.listPaths(src, thisListing.getLastName(),
            needLocation);
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
    String result = file.toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
          file+" is not a valid DFS filename.");
    }
    return result;
  }
}
