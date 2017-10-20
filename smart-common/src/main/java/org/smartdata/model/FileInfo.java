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
package org.smartdata.model;

import java.util.Objects;

public class FileInfo {
  private String path;
  private long fileId;
  private long length;
  private boolean isdir;
  private short blockReplication;
  private long blocksize;
  private long modificationTime;
  private long accessTime;
  private short permission;
  private String owner;
  private String group;
  private byte storagePolicy;

  public FileInfo(String path, long fileId, long length, boolean isdir,
      short blockReplication, long blocksize, long modificationTime,
      long accessTime, short permission, String owner, String group,
      byte storagePolicy) {
    this.path = path;
    this.fileId = fileId;
    this.length = length;
    this.isdir = isdir;
    this.blockReplication = blockReplication;
    this.blocksize = blocksize;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.permission = permission;
    this.owner = owner;
    this.group = group;
    this.storagePolicy = storagePolicy;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getFileId() {
    return fileId;
  }

  public void setFileId(long fileId) {
    this.fileId = fileId;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public boolean isdir() {
    return isdir;
  }

  public void setIsdir(boolean isdir) {
    this.isdir = isdir;
  }

  public short getBlockReplication() {
    return blockReplication;
  }

  public void setBlockReplication(short blockReplication) {
    this.blockReplication = blockReplication;
  }

  public long getBlocksize() {
    return blocksize;
  }

  public void setBlocksize(long blocksize) {
    this.blocksize = blocksize;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  public short getPermission() {
    return permission;
  }

  public void setPermission(short permission) {
    this.permission = permission;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public byte getStoragePolicy() {
    return storagePolicy;
  }

  public void setStoragePolicy(byte storagePolicy) {
    this.storagePolicy = storagePolicy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileInfo fileInfo = (FileInfo) o;
    return fileId == fileInfo.fileId
        && length == fileInfo.length
        && isdir == fileInfo.isdir
        && blockReplication == fileInfo.blockReplication
        && blocksize == fileInfo.blocksize
        && modificationTime == fileInfo.modificationTime
        && accessTime == fileInfo.accessTime
        && permission == fileInfo.permission
        && storagePolicy == fileInfo.storagePolicy
        && Objects.equals(path, fileInfo.path)
        && Objects.equals(owner, fileInfo.owner)
        && Objects.equals(group, fileInfo.group);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        path,
        fileId,
        length,
        isdir,
        blockReplication,
        blocksize,
        modificationTime,
        accessTime,
        permission,
        owner,
        group,
        storagePolicy);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return String.format(
        "FileInfo{path=\'%s\', fileId=%s, length=%s, isdir=%s, blockReplication=%s, "
            + "blocksize=%s, modificationTime=%s, accessTime=%s, permission=%s, owner=\'%s\', "
            + "group=\'%s\', storagePolicy=%s}",
        path,
        fileId,
        length,
        isdir,
        blockReplication,
        blocksize,
        modificationTime,
        accessTime,
        permission,
        owner,
        group,
        storagePolicy);
  }

  public static class Builder {
    private String path;
    private long fileId;
    private long length;
    private boolean isdir;
    private short blockReplication;
    private long blocksize;
    private long modificationTime;
    private long accessTime;
    private short permission;
    private String owner;
    private String group;
    private byte storagePolicy;

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setFileId(long fileId) {
      this.fileId = fileId;
      return this;
    }

    public Builder setLength(long length) {
      this.length = length;
      return this;
    }

    public Builder setIsdir(boolean isdir) {
      this.isdir = isdir;
      return this;
    }

    public Builder setBlockReplication(short blockReplication) {
      this.blockReplication = blockReplication;
      return this;
    }

    public Builder setBlocksize(long blocksize) {
      this.blocksize = blocksize;
      return this;
    }

    public Builder setModificationTime(long modificationTime) {
      this.modificationTime = modificationTime;
      return this;
    }

    public Builder setAccessTime(long accessTime) {
      this.accessTime = accessTime;
      return this;
    }

    public Builder setPermission(short permission) {
      this.permission = permission;
      return this;
    }

    public Builder setOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder setGroup(String group) {
      this.group = group;
      return this;
    }

    public Builder setStoragePolicy(byte storagePolicy) {
      this.storagePolicy = storagePolicy;
      return this;
    }

    public FileInfo build() {
      return new FileInfo(path, fileId, length, isdir, blockReplication,
          blocksize, modificationTime, accessTime, permission, owner,
          group, storagePolicy);
    }

    @Override
    public String toString() {
      return String.format(
          "Builder{path=\'%s\', fileId=%s, length=%s, isdir=%s, blockReplication=%s, "
              + "blocksize=%s, modificationTime=%s, accessTime=%s, permission=%s, owner=\'%s\', "
              + "group=\'%s\', storagePolicy=\'%s\'}",
          path,
          fileId,
          length,
          isdir,
          blockReplication,
          blocksize,
          modificationTime,
          accessTime,
          permission,
          owner,
          group,
          storagePolicy);
    }
  }
}
