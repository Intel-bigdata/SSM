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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.FileInfo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Contain utils related to hadoop cluster.
 */
public class HadoopUtil {

  public static URI getNameNodeUri(Configuration conf)
      throws IOException {
    String nnRpcAddr = null;

    String[] rpcAddrKeys = {
        SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY, // Keep it first
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY
    };

    String[] nnRpcAddrs = new String[rpcAddrKeys.length];

    int lastNotNullIdx = 0;
    for (int index = 0; index < rpcAddrKeys.length; index++) {
      nnRpcAddrs[index] = conf.get(rpcAddrKeys[index]);
      lastNotNullIdx = nnRpcAddrs[index] == null ? lastNotNullIdx : index;
      nnRpcAddr = nnRpcAddr == null ? nnRpcAddrs[index] : nnRpcAddr;
    }

    if (nnRpcAddr == null) {
      throw new IOException("Can not find NameNode RPC server address. "
          + "Please configure it through '"
          + SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY + "'.");
    }

    if (lastNotNullIdx == 0 && rpcAddrKeys.length > 1) {
      conf.set(rpcAddrKeys[1], nnRpcAddr);
    }

    try {
      return new URI(nnRpcAddr);
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI Syntax: " + nnRpcAddr, e);
    }
  }

  public static FileInfo convertFileStatus(HdfsFileStatus status, String path) {
    return FileInfo.newBuilder()
      .setPath(path)
      .setFileId(status.getFileId())
      .setLength(status.getLen())
      .setIsdir(status.isDir())
      .setBlock_replication(status.getReplication())
      .setBlocksize(status.getBlockSize())
      .setModification_time(status.getModificationTime())
      .setAccess_time(status.getAccessTime())
      .setPermission(status.getPermission().toShort())
      .setOwner(status.getOwner())
      .setGroup(status.getGroup())
      .setStoragePolicy(status.getStoragePolicy())
      .build();
  }
}
