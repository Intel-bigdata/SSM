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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.SmartInputStream;
import org.apache.hadoop.hdfs.SmartStripedInputStream;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.InotifyProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.security.token.Token;
import org.smartdata.hdfs.action.move.DBlock;
import org.smartdata.hdfs.action.move.StorageGroup;
import org.smartdata.hdfs.action.move.DBlockStriped;
import org.smartdata.model.FileState;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompatibilityHelper31 implements CompatibilityHelper {
  @Override
  public String[] getStorageTypes(LocatedBlock lb) {
    List<String> types = new ArrayList<>();
    for (StorageType type : lb.getStorageTypes()) {
      types.add(type.toString());
    }
    return types.toArray(new String[types.size()]);
  }

  @Override
  public void replaceBlock(
      DataOutputStream out,
      ExtendedBlock eb,
      String storageType,
      Token<BlockTokenIdentifier> accessToken,
      String dnUUID,
      DatanodeInfo info)
      throws IOException {
    new Sender(out).replaceBlock(eb, StorageType.valueOf(storageType), accessToken, dnUUID, info, null);
  }

  @Override
  public String[] getMovableTypes() {
    List<String> types = new ArrayList<>();
    for (StorageType type : StorageType.getMovableTypes()) {
      types.add(type.toString());
    }
    return types.toArray(new String[types.size()]);
  }

  @Override
  public String getStorageType(StorageReport report) {
    return report.getStorage().getStorageType().toString();
  }

  @Override
  public List<String> chooseStorageTypes(BlockStoragePolicy policy, short replication) {
    List<String> types = new ArrayList<>();
    for (StorageType type : policy.chooseStorageTypes(replication)) {
      types.add(type.toString());
    }
    return types;
  }

  @Override
  public boolean isMovable(String type) {
    return StorageType.valueOf(type).isMovable();
  }

  @Override
  public DatanodeInfo newDatanodeInfo(String ipAddress, int xferPort) {
    DatanodeID datanodeID = new DatanodeID(ipAddress, null, null,
        xferPort, 0, 0, 0);
    DatanodeDescriptor datanodeDescriptor = new DatanodeDescriptor(datanodeID);
    return datanodeDescriptor;
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
      return getDFSClientAppend(client, dest, buffersize);
    }
    return client.create(dest, true);
  }

  @Override
  public OutputStream getDFSClientAppend(DFSClient client, String dest,
                                         int buffersize) throws IOException {
    return client
        .append(dest, buffersize,
            EnumSet.of(CreateFlag.APPEND), null, null);
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
  public int getReadTimeOutConstant() {
    return HdfsConstants.READ_TIMEOUT;
  }

  @Override
  public Token<BlockTokenIdentifier> getAccessToken(
      KeyManager km, ExtendedBlock eb, StorageGroup target) throws IOException {
    return km.getAccessToken(eb, new StorageType[]{StorageType.parseStorageType(target.getStorageType())}, new String[0]);
  }

  @Override
  public int getIOFileBufferSize(Configuration conf) {
    return conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
  }

  @Override
  public InputStream getVintPrefixed(DataInputStream in) throws IOException {
    return PBHelperClient.vintPrefixed(in);
  }

  @Override
  public LocatedBlocks getLocatedBlocks(HdfsLocatedFileStatus status) {
    return status.getLocatedBlocks();
  }

  @Override
  public HdfsFileStatus createHdfsFileStatus(
      long length, boolean isdir, int block_replication, long blocksize, long modification_time,
      long access_time, FsPermission permission, String owner, String group, byte[] symlink, byte[] path,
      long fileId, int childrenNum, FileEncryptionInfo feInfo, byte storagePolicy) {
    return new HdfsFileStatus.Builder()
        .length(length)
        .isdir(isdir)
        .replication(block_replication)
        .blocksize(blocksize)
        .mtime(modification_time)
        .atime(access_time)
        .perm(permission)
        .owner(owner)
        .group(group)
        .symlink(symlink)
        .path(path)
        .fileId(fileId)
        .children(childrenNum)
        .feInfo(feInfo)
        .storagePolicy(storagePolicy)
        .build();
  }

  @Override
  public byte getErasureCodingPolicy(HdfsFileStatus fileStatus) {
    ErasureCodingPolicy erasureCodingPolicy = fileStatus.getErasureCodingPolicy();
    // null means replication policy and its id is 0 in HDFS.
    if (erasureCodingPolicy == null) {
      return (byte) 0;
    }
    return fileStatus.getErasureCodingPolicy().getId();
  }

  @Override
  public byte getErasureCodingPolicyByName(DFSClient client, String ecPolicyName) throws IOException {
    if (ecPolicyName.equals(SystemErasureCodingPolicies.getReplicationPolicy().getName())) {
      return (byte) 0;
    }
    for (ErasureCodingPolicyInfo policyInfo : client.getErasureCodingPolicies()) {
      if (policyInfo.getPolicy().getName().equals(ecPolicyName)) {
        return policyInfo.getPolicy().getId();
      }
    }
    return (byte) -1;
  }

  @Override
  public Map<Byte, String> getErasureCodingPolicies(DFSClient dfsClient) throws IOException {
    Map<Byte, String> policies = new HashMap<>();
    /**
     * The replication policy is excluded by the get method of client,
     * but it should also be put. Its id is always 0.
     */
    policies.put((byte) 0, SystemErasureCodingPolicies.getReplicationPolicy().getName());
    for (ErasureCodingPolicyInfo policyInfo : dfsClient.getErasureCodingPolicies()) {
      ErasureCodingPolicy policy = policyInfo.getPolicy();
      policies.put(policy.getId(), policy.getName());
    }
    return policies;
  }


  @Override
  public List<String> getStorageTypeForEcBlock(
      LocatedBlock lb, BlockStoragePolicy policy, byte policyId) throws IOException {
    if (lb.isStriped()) {
      //TODO: verify the current storage policy (policyID) or the target one
      //TODO: output log for unsupported storage policy for EC block
      String policyName = policy.getName();
      // Exclude onessd/onedisk action to be executed on EC block.
      // EC blocks can only be put on a same storage medium.
      if (policyName.equalsIgnoreCase("Warm") |
          policyName.equalsIgnoreCase("One_SSD") |
          policyName.equalsIgnoreCase("Lazy_Persist")) {
        throw new IOException("onessd/onedisk/ramdisk is not applicable to EC block!");
      }
      if (ErasureCodingPolicyManager
          .checkStoragePolicySuitableForECStripedMode(policyId)) {
        return chooseStorageTypes(policy, (short) lb.getLocations().length);
      } else {
        throw new IOException("Unsupported storage policy for EC block: " + policy.getName());
      }
    }
    return null;
  }

  @Override
  public DBlock newDBlock(LocatedBlock lb, HdfsFileStatus status) {
    Block blk = lb.getBlock().getLocalBlock();
    ErasureCodingPolicy ecPolicy = status.getErasureCodingPolicy();
    DBlock db;
    if (lb.isStriped()) {
      LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
      byte[] indices = new byte[lsb.getBlockIndices().length];
      for (int i = 0; i < indices.length; i++) {
        indices[i] = (byte) lsb.getBlockIndices()[i];
      }
      db = (DBlock) new DBlockStriped(blk, indices, (short) ecPolicy.getNumDataUnits(),
          ecPolicy.getCellSize());
    } else {
      db = new DBlock(blk);
    }
    return db;
  }

  @Override
  public boolean isLocatedStripedBlock(LocatedBlock lb) {
    return lb instanceof LocatedStripedBlock;
  }

  @Override
  public DBlock getDBlock(DBlock block, StorageGroup source) {
    if (block instanceof DBlockStriped) {
      return ((DBlockStriped) block).getInternalBlock(source);
    }
    return block;
  }

  @Override
  public DFSInputStream getNormalInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      FileState fileState) throws IOException {
    LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(src, 0);
    ErasureCodingPolicy ecPolicy = locatedBlocks.getErasureCodingPolicy();
    if (ecPolicy != null) {
      return new SmartStripedInputStream(dfsClient, src, verifyChecksum, ecPolicy, locatedBlocks, fileState);
    }
    return new SmartInputStream(dfsClient, src, verifyChecksum, fileState);
  }
}