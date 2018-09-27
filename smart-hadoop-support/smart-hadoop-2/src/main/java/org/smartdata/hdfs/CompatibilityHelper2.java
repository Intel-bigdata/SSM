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
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.security.token.Token;
import org.smartdata.hdfs.action.move.DBlock;
import org.smartdata.hdfs.action.move.MLocation;
import org.smartdata.hdfs.action.move.StorageGroup;
import org.smartdata.hdfs.action.move.StorageMap;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class CompatibilityHelper2 {
  public int getReadTimeOutConstant() {
    return HdfsServerConstants.READ_TIMEOUT;
  }

  public Token<BlockTokenIdentifier> getAccessToken(
      KeyManager km, ExtendedBlock eb, StorageGroup target) throws IOException {
    return km.getAccessToken(eb);
  }

  public int getIOFileBufferSize(Configuration conf) {
    return HdfsConstants.IO_FILE_BUFFER_SIZE;
  }

  public InputStream getVintPrefixed(DataInputStream in) throws IOException {
    return PBHelper.vintPrefixed(in);
  }

  public LocatedBlocks getLocatedBlocks(HdfsLocatedFileStatus status) {
    return status.getBlockLocations();
  }

  public HdfsFileStatus createHdfsFileStatus(
      long length, boolean isdir, int block_replication, long blocksize, long modification_time,
      long access_time, FsPermission permission, String owner, String group, byte[] symlink, byte[] path,
      long fileId, int childrenNum, FileEncryptionInfo feInfo, byte storagePolicy) {
    return new HdfsFileStatus(
        length, isdir, block_replication, blocksize, modification_time, access_time, permission,
        owner, group, symlink, path, fileId, childrenNum, feInfo, storagePolicy);
  }

  public byte getErasureCodingPolicy(HdfsFileStatus fileStatus) {
    // for HDFS2.x, the erasure policy is always replication whose id is 0 in HDFS.
    return (byte) 0;
  }

  public byte getErasureCodingPolicyByName(DFSClient client, String ecPolicyName) throws IOException {
    return (byte) 0;
  }

  public Map<Byte, String> getErasureCodingPolicies(DFSClient dfsClient) throws IOException {
    return null;
  }

  public List<String> getStorageTypeForEcBlock(LocatedBlock lb, BlockStoragePolicy policy, byte policyId) {
    return null;
  }

  public DBlock newDBlock(
      LocatedBlock lb, List<MLocation> locations, StorageMap storages, HdfsFileStatus status) {
    Block blk = lb.getBlock().getLocalBlock();
    DBlock db = new DBlock(blk);
    for (MLocation ml : locations) {
      StorageGroup source = storages.getSource(ml);
      if (source != null) {
        db.addLocation(source);
      }
    }
    return db;
  }
}