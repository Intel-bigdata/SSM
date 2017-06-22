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
package org.smartdata.common.metastore;

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

public class FileInfo {
  private String path;
  private long fileId;
  private long length;
  private boolean isdir;
  private short block_replication;
  private long blocksize;
  private long modification_time;
  private long access_time;
  private short permission;
  private String owner;
  private String group;
  private byte storagePolicy;

  public FileInfo(String path, long fileId, long length, boolean isdir,
      short block_replication, long blocksize, long modification_time,
      long access_time, short permission, String owner, String group,
      byte storagePolicy) {
    this.path = path;
    this.fileId = fileId;
    this.length = length;
    this.isdir = isdir;
    this.block_replication = block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
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

  public short getBlock_replication() {
    return block_replication;
  }

  public void setBlock_replication(short block_replication) {
    this.block_replication = block_replication;
  }

  public long getBlocksize() {
    return blocksize;
  }

  public void setBlocksize(long blocksize) {
    this.blocksize = blocksize;
  }

  public long getModification_time() {
    return modification_time;
  }

  public void setModification_time(long modification_time) {
    this.modification_time = modification_time;
  }

  public long getAccess_time() {
    return access_time;
  }

  public void setAccess_time(long access_time) {
    this.access_time = access_time;
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

  public static FileInfo fromHdfsFileStatus(HdfsFileStatus hdfsFileStatus, String parent) {
    return null;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String path;
    private long fileId;
    private long length;
    private boolean isdir;
    private short block_replication;
    private long blocksize;
    private long modification_time;
    private long access_time;
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

    public Builder setBlock_replication(short block_replication) {
      this.block_replication = block_replication;
      return this;
    }

    public Builder setBlocksize(long blocksize) {
      this.blocksize = blocksize;
      return this;
    }

    public Builder setModification_time(long modification_time) {
      this.modification_time = modification_time;
      return this;
    }

    public Builder setAccess_time(long access_time) {
      this.access_time = access_time;
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
      return new FileInfo(path, fileId, length, isdir, block_replication,
          blocksize, modification_time, access_time, permission,owner,
          group, storagePolicy);
    }
  }
}
