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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FileInfoMapper {
  public static final String PATH = "path";
  public static final String FID = "fid";
  public static final String LENGTH = "length";
  public static final String BLOCK_REPLICATION = "block_replication";
  public static final String BLOCK_SIZE = "block_size";
  public static final String MODIFICATION_TIME = "modification_time";
  public static final String ACCESS_TIME = "access_time";
  public static final String IS_DIR = "is_dir";
  public static final String STORAGE_POLICY = "storage_policy";
  public static final String OWNER = "owner";
  public static final String GROUP = "group";
  public static final String PERMISSION = "permission";

  private Map<String, Object> attrMap;

  public FileInfoMapper(Map<String, Object> attrMap) {
    this.attrMap = attrMap;
  }

  public Set<String> getAttributesSpecified() {
    return attrMap.keySet();
  }

  public String getPath() {
    return (String) attrMap.get(PATH);
  }

  public Long getFileId() {
    return (Long) attrMap.get(FID);
  }

  public Long getLength() {
    return (Long) attrMap.get(LENGTH);
  }

  public Boolean getIsdir() {
    return (Boolean) attrMap.get(IS_DIR);
  }

  public Short getBlock_replication() {
    return (Short) attrMap.get(BLOCK_REPLICATION);
  }

  public Long getBlocksize() {
    return (Long) attrMap.get(BLOCK_SIZE);
  }

  public Long getModification_time() {
    return (Long) attrMap.get(MODIFICATION_TIME);
  }

  public Long getAccess_time() {
    return (Long) attrMap.get(ACCESS_TIME);
  }

  public Short getPermission() {
    return (Short) attrMap.get(PERMISSION);
  }

  public String getOwner() {
    return (String) attrMap.get(OWNER);
  }

  public String getGroup(String group) {
    return (String) attrMap.get(GROUP);
  }

  public Byte getStoragePolicy(byte storagePolicy) {
    return (Byte) attrMap.get(STORAGE_POLICY);
  }

  public FileInfo toFileInfo() {
    FileInfo.Builder builder = FileInfo.newBuilder();
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return String.format("FileInfoMapper{attrMap=%s}", attrMap);
  }

  public static class Builder {
    private Map<String, Object> attrMap = new HashMap<>();

    public Builder setPath(String path) {
      attrMap.put(FileInfoMapper.PATH, path);
      return this;
    }

    public Builder setFileId(long fileId) {
      attrMap.put(FileInfoMapper.FID, fileId);
      return this;
    }

    public Builder setLength(long length) {
      attrMap.put(FileInfoMapper.LENGTH, length);
      return this;
    }

    public Builder setIsdir(boolean isdir) {
      attrMap.put(FileInfoMapper.IS_DIR, isdir);
      return this;
    }

    public Builder setBlockReplication(short blockReplication) {
      attrMap.put(FileInfoMapper.BLOCK_REPLICATION, blockReplication);
      return this;
    }

    public Builder setBlocksize(long blocksize) {
      attrMap.put(FileInfoMapper.BLOCK_SIZE, blocksize);
      return this;
    }

    public Builder setModificationTime(long modificationTime) {
      attrMap.put(FileInfoMapper.MODIFICATION_TIME, modificationTime);
      return this;
    }

    public Builder setAccessTime(long accessTime) {
      attrMap.put(FileInfoMapper.ACCESS_TIME, accessTime);
      return this;
    }

    public Builder setPermission(short permission) {
      attrMap.put(FileInfoMapper.PERMISSION, permission);
      return this;
    }

    public Builder setOwner(String owner) {
      attrMap.put(FileInfoMapper.OWNER, owner);
      return this;
    }

    public Builder setGroup(String group) {
      attrMap.put(FileInfoMapper.GROUP, group);
      return this;
    }

    public Builder setStoragePolicy(byte storagePolicy) {
      attrMap.put(FileInfoMapper.STORAGE_POLICY, storagePolicy);
      return this;
    }

    public FileInfoMapper build() {
      return new FileInfoMapper(attrMap);
    }

    @Override
    public String toString() {
      return String.format("Builder{attrMap=%s}", attrMap);
    }
  }
}
