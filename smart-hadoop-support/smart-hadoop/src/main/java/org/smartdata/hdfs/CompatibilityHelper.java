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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.SmartInputStreamFactory;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.InotifyProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.security.token.Token;
import org.smartdata.conf.SmartConf;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface CompatibilityHelper {
  String[] getStorageTypes(LocatedBlock lb);

  void replaceBlock(
      DataOutputStream out,
      ExtendedBlock eb,
      String storageType,
      Token<BlockTokenIdentifier> accessToken,
      String dnUUID,
      DatanodeInfo info)
      throws IOException;

  String[] getMovableTypes();

  String getStorageType(StorageReport report);

  List<String> chooseStorageTypes(BlockStoragePolicy policy, short replication);

  boolean isMovable(String type);

  DatanodeInfo newDatanodeInfo(String ipAddress, int xferPort);

  InotifyProtos.AppendEventProto getAppendEventProto(Event.AppendEvent event);

  Event.AppendEvent getAppendEvent(InotifyProtos.AppendEventProto proto);

  //Todo: Work-around, should remove this function in the future
  boolean truncate(DFSClient client, String src, long newLength) throws IOException;

  boolean truncate(DistributedFileSystem fileSystem, String src, long newLength) throws IOException;

  int getSidInDatanodeStorageReport(DatanodeStorage datanodeStorage);

  OutputStream getDFSClientAppend(DFSClient client, String dest, int buffersize, long offset) throws IOException;

  OutputStream getS3outputStream(String dest, Configuration conf) throws IOException;

  SmartInputStreamFactory getSmartInputStreamFactory();
}
