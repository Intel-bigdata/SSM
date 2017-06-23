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
package org.smartdata.common.models;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

public class FileStatusInternal extends HdfsFileStatus {
  private String path;

  /**
   * Constructor
   *
   * @param length            the number of bytes the file has
   * @param isdir             if the path is a directory
   * @param block_replication the replication factor
   * @param blocksize         the block size
   * @param modification_time modification time
   * @param access_time       access time
   * @param permission        permission
   * @param owner             the owner of the path
   * @param group             the group of the path
   * @param symlink
   * @param path              the local name in java UTF8 encoding the same as that in-memory
   * @param fileId            the file id
   * @param childrenNum
   * @param feInfo            the file's encryption info
   * @param storagePolicy
   */
  public FileStatusInternal(long length, boolean isdir, int block_replication,
      long blocksize, long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] symlink,
      byte[] path, String parent, long fileId, int childrenNum, FileEncryptionInfo feInfo,
      byte storagePolicy) {
    super(length, isdir, block_replication, blocksize, modification_time,
        access_time, permission, owner, group, symlink, path, fileId, childrenNum,
        feInfo, storagePolicy);
    this.path = this.getFullName(parent);
  }

  public FileStatusInternal(HdfsFileStatus status, String parent) {
    this(status.getLen(), status.isDir(), status.getReplication(),
        status.getBlockSize(), status.getModificationTime(), status.getAccessTime(),
        status.getPermission(), status.getOwner(), status.getGroup(), status.getSymlinkInBytes(),
        status.getLocalNameInBytes(), parent, status.getFileId(), status.getChildrenNum(),
        status.getFileEncryptionInfo(), status.getStoragePolicy());
  }

  public FileStatusInternal(HdfsFileStatus status) {
    this(status, "");
  }

  public String getPath() {
    return this.path;
  }

  public void setPath(String path) {
    this.path = path;
  }
}
