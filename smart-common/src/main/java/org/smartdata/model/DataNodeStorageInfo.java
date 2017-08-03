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

public class DataNodeStorageInfo {

  private String uuid;
  private short sid;
  private short state;
  private String storageId;
  private short failed;
  private long capacity;
  private long dfsUsed;
  private long remaining;
  private long blockPool;

  public DataNodeStorageInfo(String uuid, short sid, short state,
      String storageId, short failed, long capacity,
      long dfsUsed, long remaining, long blockPool) {
    this.uuid = uuid;
    this.sid = sid;
    this.state = state;
    this.storageId = storageId;
    this.failed = failed;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPool = blockPool;
  }

  /**getters & setters**/
  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public short getSid() {
    return sid;
  }

  public void setSid(short sid) {
    this.sid = sid;
  }

  public short getState() {
    return state;
  }

  public void setState(short state) {
    this.state = state;
  }

  public String getStorageId() {
    return storageId;
  }

  public void setStorageId(String storageId) {
    this.storageId = storageId;
  }

  public short getFailed() {
    return failed;
  }

  public void setFailed(short failed) {
    this.failed = failed;
  }

  public long getCapacity() {
    return capacity;
  }

  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  public long getDfsUsed() {
    return dfsUsed;
  }

  public void setDfsUsed(long dfsUsed) {
    this.dfsUsed = dfsUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  public void setRemaining(long remaining) {
    this.remaining = remaining;
  }

  public long getBlockPool() {
    return blockPool;
  }

  public void setBlockPool(long blockPool) {
    this.blockPool = blockPool;
  }

  @Override
  public int hashCode() {
    int result = (int) (uuid != null ? uuid.hashCode() : 0);
    result = 31 * result + (int) sid;
    result = 31 * result + (int) state;
    result = 31 * result + (storageId != null ? storageId.hashCode() : 0);
    result = 31 * result + (int) failed;
    result = 31 * result + (int) (capacity ^ (capacity >>> 32));
    result = 31 * result + (int) (dfsUsed ^ (dfsUsed >>> 32));
    result = 31 * result + (int) (remaining ^ (remaining >>> 32));
    result = 31 * result + (int) (blockPool ^ (blockPool >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
        return true;
    }
    if (!(o instanceof DataNodeStorageInfo)) {
        return false;
    }
  DataNodeStorageInfo dataNodeStorageInfo = (DataNodeStorageInfo) o;

    if (getUuid() != dataNodeStorageInfo.getUuid()) {
        return false;
    }
    if (getSid() != dataNodeStorageInfo.getSid()) {
        return false;
    }
    if (getState() != dataNodeStorageInfo.getState()) {
        return false;
    }
    if (getStorageId() != dataNodeStorageInfo.getStorageId()) {
        return false;
    }
    if (getFailed() != dataNodeStorageInfo.getFailed()) {
        return false;
    }
    if (getCapacity() != dataNodeStorageInfo.getCapacity()) {
        return false;
    }
    if (getDfsUsed() != dataNodeStorageInfo.getDfsUsed()) {
        return false;
    }
    if (getRemaining() != dataNodeStorageInfo.getRemaining()) {
      return false;
    }
    if (getBlockPool() != dataNodeStorageInfo.getBlockPool()) {
      return false;
    }
      return true;
  }

  @Override
  public String toString() {
    return String.format("DataNodeStorageInfo{uuid=%s, sid=%s, " +
        "state=%d, storage_id=%s, failed=%d, capacity=%d, " +
        "dfs_used=%d, remaining=%d, block_pool=%d}",
        uuid, sid, state, storageId, failed,
        capacity, dfsUsed, remaining, blockPool);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String uuid;
    private short sid;
    private short state;
    private String storageId;
    private short failed;
    private long capacity;
    private long dfsUsed;
    private long remaining;
    private long blockPool;

    public Builder setUuid(String uuid) {
      this.uuid = uuid;
      return this;
    }
    public Builder setSid(short sid) {
      this.sid = sid;
      return this;
    }
    public Builder setState(short state) {
      this.state = state;
      return this;
    }
    public Builder setStorageId(String storageId) {
      this.storageId = storageId;
      return this;
    }
    public Builder setFailed(short failed) {
      this.failed = failed;
      return this;
    }
    public Builder setCapacity(long capacity) {
      this.capacity = capacity;
      return this;
    }
    public Builder setDfsUsed(long dfsUsed) {
      this.dfsUsed = dfsUsed;
      return this;
    }
    public Builder setRemaining(long remaining) {
      this.remaining = remaining;
      return this;
    }
    public Builder setBlockPool(long blockPool) {
      this.blockPool = blockPool;
      return this;
    }

    public DataNodeStorageInfo build() {
      return new DataNodeStorageInfo(uuid, sid, state, storageId, failed,
          capacity, dfsUsed, remaining, blockPool);
    }
  }
}
