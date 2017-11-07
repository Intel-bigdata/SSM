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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.*;
import org.smartdata.client.SmartClient;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.SmartFileCompressionInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

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
  String compressDir;
  
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
    compressDir = conf.get(SmartConfKeys.SMART_COMPRESSION_DIR_KEY,
        SmartConfKeys.SMART_COMPRESSION_DIR_DEFAULT);
  }
  
  private boolean iscompressFileDir(String file) {
    if (file.startsWith(compressDir)) {
      return true;
    }
    return false;
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
  /*
  public OutputStream smartCreate(String src, boolean overwrite)
      throws IOException {
    return create(src, overwrite, getDefaultReplication(),
        getDefaultBlockSize(), null);
  }
  
  public OutputStream smartCreate(String src,
      boolean overwrite,
      Progressable progress) throws IOException {
    return smartCreate(src, overwrite, getDefaultReplication(),
        getDefaultBlockSize(), progress);
  }

  public OutputStream smartCreate(String src,
      boolean overwrite,
      short replication,
      long blockSize) throws IOException {
    return create(src, overwrite, replication, blockSize, null);
  }

  public OutputStream smartCreate(String src, boolean overwrite, 
      short replication, long blockSize, Progressable progress)
      throws IOException {
    return smartCreate(src, overwrite, replication, blockSize, progress,
        dfsClientConf.ioBufferSize);
  }

  public OutputStream smartCreate(String src,
      boolean overwrite,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize)
      throws IOException {

  }

  public OutputStream smartCreate(String src,
      FsPermission permission,
      EnumSet<CreateFlag> flag,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize,
      Options.ChecksumOpt checksumOpt)
      throws IOException {
    return create(src, permission, flag, true,
        replication, blockSize, progress, buffersize, checksumOpt, null);
  }

  public OutputStream smartCreate(String src,
      FsPermission permission,
      EnumSet<CreateFlag> flag,
      boolean createParent,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize,
      Options.ChecksumOpt checksumOpt) throws IOException {
    return create(src, permission, flag, createParent, replication, blockSize,
        progress, buffersize, checksumOpt, null);
  }


  public OutputStream smartCreate(String src,
      FsPermission permission,
      EnumSet<CreateFlag> flag,
      boolean createParent,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize,
      Options.ChecksumOpt checksumOpt,
      InetSocketAddress[] favoredNodes) throws IOException {
    if (src.startsWith(compressPath)) {
      if (permission == null) {
        permission = FsPermission.getFileDefault();
      }
      FsPermission uMask = FsPermission.getUMask(conf);
      FsPermission masked = permission.applyUMask(uMask);
      return SmartDFSOutputStream.newStreamForCreate(this, src, masked,
          flag, createParent, replication, blockSize, progress, buffersize,
          null, 
          getFavoredNodesStr(favoredNodes), conf);
    } else {
      return super.create(src, permission, flag, createParent, replication, 
          blockSize, progress, buffersize, checksumOpt, favoredNodes);
    }
  }

  private String[] getFavoredNodesStr(InetSocketAddress[] favoredNodes) {
    String[] favoredNodeStrs = null;
    if (favoredNodes != null) {
      favoredNodeStrs = new String[favoredNodes.length];
      for (int i = 0; i < favoredNodes.length; i++) {
        favoredNodeStrs[i] =
            favoredNodes[i].getHostName() + ":"
                + favoredNodes[i].getPort();
      }
    }
    return favoredNodeStrs;
  }
*/
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
    if(!iscompressFileDir(src)){
      LOG.info("Uncompressed file " + src + " opened.");
      is = super.open(src, buffersize, verifyChecksum);
    }else{
      LOG.info("Compressed file " + src + " opened.");
      SmartFileCompressionInfo compressionInfo = new SmartFileCompressionInfo(
          src, 256 * 1024);
      is = new SmartDFSInputStream(this, src, verifyChecksum, compressionInfo);
    }
    reportFileAccessEvent(src);
    return  is;
  }

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
