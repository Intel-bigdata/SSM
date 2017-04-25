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
package org.apache.hadoop.hdfs.protocol;

import java.util.LinkedList;
import java.util.List;

public class FilesInfo {
  public static final int LENGTH = 0x1;
  public static final int ISDIR = 0x2;
  public static final int BLOCK_REPLICATION = 0x4;
  public static final int BLOCK_SIZE = 0x8;
  public static final int MODIFICATION_TIME = 0x10;
  public static final int ACCESS_TIME = 0x20;
  public static final int OWNER = 0x40;
  public static final int GROUP = 0x80;
  public static final int FILEID = 0x100;
  public static final int CHILDRENNUM = 0x200;
  public static final int STORAGEPOLICY = 0x400;


  public static final int ALL = (STORAGEPOLICY << 1) - 1;

  private List<String> allPaths;
  //private final byte[] path;
  //private final byte[] symlink; // symlink target encoded in java UTF8 or null
  private List<Long> length;
  private List<Boolean> isdir;
  private List<Short> blockReplication;
  private List<Long> blockSize;
  private List<Long> modificationTime;
  private List<Long> accessTime;
  //private final FsPermission permission;
  private List<String> owner;
  private List<String> group;
  private List<Long> fileId;
  //private final FileEncryptionInfo feInfo;
  //private final ErasureCodingPolicy ecPolicy;
  // Used by dir, not including dot and dotdot. Always zero for a regular file.
  private List<Integer> childrenNum;
  private List<Byte> storagePolicy;

  public List<String> getAllPaths() {
    return allPaths;
  }

  public void setAllPaths(List<String> allPaths) {
    this.allPaths = allPaths;
  }

  public List<Long> getLength() {
    return length;
  }

  public void setLength(List<Long> length) {
    this.length = length;
  }

  public List<Boolean> getIsdir() {
    return isdir;
  }

  public void setIsdir(List<Boolean> isdir) {
    this.isdir = isdir;
  }

  public List<Short> getBlockReplication() {
    return blockReplication;
  }

  public void setBlockReplication(List<Short> blockReplication) {
    this.blockReplication = blockReplication;
  }

  public List<Long> getBlocksize() {
    return blockSize;
  }

  public void setBlocksize(List<Long> blockSize) {
    this.blockSize = blockSize;
  }

  public void setModificationTime(List<Long> modificationTime) {
    this.modificationTime = modificationTime;
  }

  public List<Long> getModificationTime() {
    return modificationTime;
  }

  public void setAccessTime(List<Long> accessTime) {
    this.accessTime = accessTime;
  }

  public List<Long> getAccessTime() {
    return accessTime;
  }

  public void setOwner(List<String> owner) {
    this.owner = owner;
  }

  public List<String> getOwner() {
    return owner;
  }

  public void setGroup(List<String> group) {
    this.group = group;
  }

  public List<String> getGroup() {
    return group;
  }

  public void setFileId(List<Long> fileId) {
    this.fileId = fileId;
  }

  public List<Long> getFileId() {
    return fileId;
  }

  public List<Integer> getChildrenNum() {
    return childrenNum;
  }

  public void setChildrenNum(List<Integer> childrenNum) {
    this.childrenNum = childrenNum;
  }

  public List<Byte> getStoragePolicy() {
    return storagePolicy;
  }

  public void setStoragePolicy(List<Byte> storagePolicy) {
    this.storagePolicy = storagePolicy;
  }

  private int vaildItems = 0;

  public FilesInfo(int itemTypes) {
    vaildItems = itemTypes;
    allPaths = new LinkedList<>();

    this.length = new LinkedList<>();
    this.isdir = new LinkedList<>();
    this.blockReplication = new LinkedList<>();
    this.blockSize = new LinkedList<>();
    this.modificationTime = new LinkedList<>();
    this.accessTime = new LinkedList<>();
    this.owner = new LinkedList<>();
    this.group = new LinkedList<>();
    this.fileId = new LinkedList<>();
    this.childrenNum = new LinkedList<>();
    this.storagePolicy = new LinkedList<>();
  }

  public void addPath(String p) {
    allPaths.add(p);
  }

  private void doAddValue(int type, Object value) {
    switch (type) {
      case LENGTH:
        length.add((Long)value);
        break;
      case ISDIR:
        isdir.add((Boolean)value);
        break;
      case BLOCK_REPLICATION:
        blockReplication.add((Short)value);
        break;
      case BLOCK_SIZE:
        blockSize.add((Long)value);
        break;
      case MODIFICATION_TIME:
        modificationTime.add((Long)value);
        break;
      case ACCESS_TIME:
        accessTime.add((Long)value);
        break;
      case OWNER:
        owner.add((String)value);
        break;
      case GROUP:
        group.add((String)value);
        break;
      case FILEID:
        fileId.add((Long)value);
        break;
      case CHILDRENNUM:
        childrenNum.add((Integer)value);
        break;
      case STORAGEPOLICY:
        storagePolicy.add((Byte)value);
        break;
    }
  }

  public void addValue(int types, HdfsFileStatus fileStatus) {
    for (int i = 0; i < 32; i++) {
      int type = types & (1 << i);
      if (type == 0) {
        continue;
      }

      switch (type) {
        case LENGTH:
          length.add(fileStatus.getLen());
          break;
        case ISDIR:
          isdir.add(fileStatus.isDir());
          break;
        case BLOCK_REPLICATION:
          blockReplication.add(fileStatus.getReplication());
          break;
        case BLOCK_SIZE:
          blockSize.add(fileStatus.getBlockSize());
          break;
        case MODIFICATION_TIME:
          modificationTime.add(fileStatus.getModificationTime());
          break;
        case ACCESS_TIME:
          accessTime.add(fileStatus.getAccessTime());
          break;
        case OWNER:
          owner.add(fileStatus.getOwner());
          break;
        case GROUP:
          group.add(fileStatus.getGroup());
          break;
        case FILEID:
          fileId.add(fileStatus.getFileId());
          break;
        case CHILDRENNUM:
          childrenNum.add(fileStatus.getChildrenNum());
          break;
        case STORAGEPOLICY:
          storagePolicy.add(fileStatus.getStoragePolicy());
          break;
      }
    }
  }

  public boolean isValid(int toTest) {
    return (toTest & vaildItems) == toTest;
  }

  public int getVaildItems() {
    return vaildItems;
  }
}
