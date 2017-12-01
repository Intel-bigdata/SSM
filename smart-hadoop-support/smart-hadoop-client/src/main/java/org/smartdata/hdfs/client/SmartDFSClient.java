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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.smartdata.client.SmartClient;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.SmartFileCompressionInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

public class SmartDFSClient extends DFSClient {
  private SmartClient smartClient = null;
  private boolean healthy = false;
  
  String compressionImpl;
  short defaultReplication;
  long defaultBlockSize;
  int ioBufferSize;
  
  private void initConf(Configuration conf) {
    compressionImpl = conf.get(SmartConfKeys.SMART_COMPRESSION_IMPL,
        SmartConfKeys.SMART_COMPRESSION_IMPL_DEFAULT);
    defaultReplication = (short) conf.getInt(
        DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    defaultBlockSize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY,
        DFS_BLOCK_SIZE_DEFAULT);
    ioBufferSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  }

  public SmartDFSClient(InetSocketAddress nameNodeAddress, Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeAddress, conf);
    initConf(conf);
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(URI nameNodeUri, Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeUri, conf);
    initConf(conf);
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
    initConf(conf);
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
    initConf(conf);
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
    initConf(conf);
    try {
      smartClient = new SmartClient(conf);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public boolean isFileCompressed(String path) throws IOException {
    return smartClient.fileCompressed(path);
  }

  public SmartFileCompressionInfo getFileCompressionInfo(String path) throws IOException {
    return smartClient.getFileCompressionInfo(path);
  }

  @Override
  public DFSInputStream open(String src)
      throws IOException, UnresolvedLinkException {
    return super.open(src);
  }

  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    DFSInputStream is;
    if (!isFileCompressed(src)) {
      LOG.info("Uncompressed file " + src + " opened.");
      is = super.open(src, buffersize, verifyChecksum);
    } else {
      LOG.info("Compressed file " + src + " opened.");
      SmartFileCompressionInfo compressionInfo = smartClient.getFileCompressionInfo(src);
      is = new CompressionDFSInputStream(this, src, verifyChecksum, compressionInfo);
    }
    reportFileAccessEvent(src);
    return  is;
  }

  /*@Override
  public LocatedBlocks getLocatedBlocks(String src, long start, long length)
      throws IOException {
    if (!isFileCompressed(src)) {
      return super.getLocatedBlocks(src, start, length);
    }

    SmartFileCompressionInfo compressionInfo = smartClient.getFileCompressionInfo(src);
    Long[] originalPos = compressionInfo.getOriginalPos().toArray(new Long[0]);
    Long[] compressedPos = compressionInfo.getCompressedPos().toArray(new Long[0]);
    int startIndex = compressionInfo.getPosIndexByOriginalOffset(start);
    int endIndex = compressionInfo.getPosIndexByOriginalOffset(start + length - 1);
    long compressedStart = compressedPos[startIndex];
    long compressedLength = 0;
    if (endIndex < compressedPos.length - 1) {
      compressedLength = compressedPos[endIndex + 1] - compressedStart;
    } else {
      compressedLength = compressionInfo.getCompressedLength() - compressedStart;
    }

    LocatedBlocks originalLocatedBlocks = super.getLocatedBlocks(src, compressedStart, compressedLength);

    List<LocatedBlock> blocks = new ArrayList<>();
    for (LocatedBlock block : originalLocatedBlocks.getLocatedBlocks()) {

      blocks.add(new LocatedBlock(
          block.getBlock(),
          block.getLocations(),
          block.getStorageIDs(),
          block.getStorageTypes(),
          compressionInfo.getPosIndexByCompressedOffset(block.getStartOffset()),
          block.isCorrupt(),
          block.getCachedLocations()
      ));
    }
    LocatedBlock lastLocatedBlock = originalLocatedBlocks.getLastLocatedBlock();
    long fileLength = compressionInfo.getOriginalLength();

    return new LocatedBlocks(fileLength,
        originalLocatedBlocks.isUnderConstruction(),
        blocks,
        lastLocatedBlock,
        originalLocatedBlocks.isLastBlockComplete(),
        originalLocatedBlocks.getFileEncryptionInfo());
  }*/

  /*
  // Not complete
  @Override
  public BlockLocation[] getBlockLocations(String src, long start,
      long length) throws IOException, UnresolvedLinkException {
    if (!isFileCompressed(src)) {
      return super.getBlockLocations(src, start, length);
    }
    BlockLocation[] blockLocations = super.getBlockLocations(src, start, length);
    return null;
  }

  @Override
  public DirectoryListing listPaths(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    if (!isFileCompressed(src)) {
      return super.listPaths(src, startAfter, needLocation);
    }
    return null;
  }

  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    if (!isFileCompressed(src)) {
      return super.getFileInfo(src);
    }
    return null;
  }
  */

  @Deprecated
  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum, FileSystem.Statistics stats)
      throws IOException, UnresolvedLinkException {
    return super.open(src,buffersize,verifyChecksum,stats);
  }

  private void reportFileAccessEvent(String src) {
    try {
      if (!healthy) {
        return;
      }
      smartClient.reportFileAccessEvent(new FileAccessEvent(src));
    } catch (IOException e) {
      // Here just ignores that failed to report
      LOG.error("Cannot report file access event to SmartServer: " + src
          + " , for: " + e.getMessage()
          + " , report mechanism will be disabled now in this instance.");
      healthy = false;
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
      }finally {
        healthy = false;
      }
    }
  }
}
