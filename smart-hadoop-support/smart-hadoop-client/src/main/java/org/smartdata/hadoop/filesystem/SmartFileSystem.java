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
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;

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
 *    Add property "smart.server.rpc.adddress" to point to Smart Server.
 *    <property>
 *      <name>smart.server.rpc.address</name>
 *      <value>127.0.0.1:7042</value>
 *    </property>
 *
 * 4. Restart HDFS
 */

public class SmartFileSystem extends DistributedFileSystem {
  private SmartDFSClient smartClient;
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

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus oldStatus = super.getFileStatus(f);
    FileState fileState = smartClient.getFileState(f.toString());
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
    ArrayList<FileStatus> newStatus = new ArrayList<>(16);
    for (FileStatus status : oldStatus) {
      FileState fileState = smartClient.getFileState(
          Path.getPathWithoutSchemeAndAuthority(status.getPath()).toString());
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
    FileState fileState = smartClient.getFileState(p.toString());
    if (fileState instanceof CompactFileState) {
      String containerFile = ((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath();
      long offset = ((CompactFileState) fileState).getFileContainerInfo().getOffset();
      return super.getFileBlockLocations(new Path(containerFile), offset + start, len);
    } else {
      return super.getFileBlockLocations(p, start, len);
    }
  }

  @Override
  public boolean isFileClosed(final Path src) throws IOException {
    FileState fileState = smartClient.getFileState(src.toString());
    if (fileState instanceof CompactFileState) {
      String containerFile = ((CompactFileState) fileState).getFileContainerInfo().getContainerFilePath();
      return super.isFileClosed(new Path(containerFile));
    } else {
      return super.isFileClosed(src);
    }
  }

  @Override
  public boolean delete(Path f, final boolean recursive) throws IOException {
    return smartClient.delete(f.toString(), recursive);
  }

  @Override
  public boolean truncate(Path f, final long newLength) throws IOException {
    return smartClient.truncate(f.toString(), newLength);
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return smartClient.rename(src.toString(), dst.toString());
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, final Options.Rename... options)
      throws IOException {
    smartClient.rename(src.toString(), dst.toString(), options);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } catch (IOException e) {
      throw e;
    } finally {
      if (smartClient != null) {
        this.smartClient.close();
      }
    }
  }
}
