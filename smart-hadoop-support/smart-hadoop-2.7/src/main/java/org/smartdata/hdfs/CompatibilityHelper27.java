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
package org.smartdata.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.InotifyProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.conf.Configuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class CompatibilityHelper27 implements CompatibilityHelper {

  public String[] getStorageTypes(LocatedBlock lb) {
    List<String> types = new ArrayList<>();
    for(StorageType type : lb.getStorageTypes()) {
      types.add(type.toString());
    }
    return types.toArray(new String[types.size()]);
  }

  public void replaceBlock(
      DataOutputStream out,
      ExtendedBlock eb,
      String storageType,
      Token<BlockTokenIdentifier> accessToken,
      String dnUUID,
      DatanodeInfo info)
      throws IOException {
    new Sender(out).replaceBlock(eb, StorageType.valueOf(storageType), accessToken, dnUUID, info);
  }

  public String[] getMovableTypes() {
    List<String> types = new ArrayList<>();
    for(StorageType type : StorageType.getMovableTypes()) {
      types.add(type.toString());
    }
    return types.toArray(new String[types.size()]);
  }

  public String getStorageType(StorageReport report) {
    return report.getStorage().getStorageType().toString();
  }

  public List<String> chooseStorageTypes(BlockStoragePolicy policy, short replication) {
    List<String> types = new ArrayList<>();
    for(StorageType type : policy.chooseStorageTypes(replication)) {
      types.add(type.toString());
    }
    return types;
  }

  public boolean isMovable(String type) {
    return StorageType.valueOf(type).isMovable();
  }

  @Override
  public DatanodeInfo newDatanodeInfo(String ipAddress, int xferPort) {
    return new DatanodeInfo(
      ipAddress,
      null,
      null,
      xferPort,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      null,
      null);
  }

  @Override
  public InotifyProtos.AppendEventProto getAppendEventProto(Event.AppendEvent event) {
    return InotifyProtos.AppendEventProto.newBuilder()
      .setPath(event.getPath())
      .setNewBlock(event.toNewBlock()).build();
  }

  @Override
  public Event.AppendEvent getAppendEvent(InotifyProtos.AppendEventProto proto) {
    return new Event.AppendEvent.Builder().path(proto.getPath())
      .newBlock(proto.hasNewBlock() && proto.getNewBlock())
      .build();
  }

  @Override
  public boolean truncate(DFSClient client, String src, long newLength) throws IOException {
    return client.truncate(src, newLength);
  }

  @Override
  public boolean truncate(DistributedFileSystem fileSystem, String src, long newLength) throws IOException {
    return fileSystem.truncate(new Path(src), newLength);
  }

  @Override
  public int getSidInDatanodeStorageReport(DatanodeStorage datanodeStorage) {
    StorageType storageType = datanodeStorage.getStorageType();
    return storageType.ordinal();
  }

  @Override
  public OutputStream getDFSClientAppend(DFSClient client, String dest,
      int buffersize, long offset) throws IOException {
    if (client.exists(dest) && offset != 0) {
      return client
          .append(dest, buffersize,
              EnumSet.of(CreateFlag.APPEND), null, null);
    }
    return client.create(dest, true);
  }

  @Override
  public OutputStream getS3outputStream(String dest, Configuration conf) throws IOException {
    // Copy to remote S3
    if (!dest.startsWith("s3")) {
      throw new IOException();
    }
    // Copy to s3
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(dest), conf);
    return fs.create(new Path(dest), true);
  }

  @Override
  public boolean setLen2Zero(DFSClient client, String src) throws IOException {
    return client.truncate(src, 0);
  }

  @Override
  public boolean setLen2Zero(DistributedFileSystem fileSystem, String src) throws IOException {
    return fileSystem.truncate(new Path(src), 0);
  }


}
