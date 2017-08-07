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
package org.smartdata.hdfs.action.move;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/** A group of storages in a datanode with the same storage type. */
public class StorageGroup {
  private final StorageType storageType;
  private DatanodeInfo datanode;

  public StorageGroup(DatanodeInfo datanode, StorageType storageType) {
    this.datanode = datanode;
    this.storageType = storageType;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public DatanodeInfo getDatanodeInfo() {
    return datanode;
  }

  /** @return the name for display */
  public String getDisplayName() {
    return datanode + ":" + storageType;
  }

  @Override
  public String toString() {
    return getDisplayName();
  }

  @Override
  public int hashCode() {
    return getStorageType().hashCode() ^ getDatanodeInfo().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || !(obj instanceof StorageGroup)) {
      return false;
    } else {
      final StorageGroup that = (StorageGroup) obj;
      return this.getStorageType() == that.getStorageType()
          && this.getDatanodeInfo().equals(that.getDatanodeInfo());
    }
  }
}
