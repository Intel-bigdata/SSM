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
  private long sid;
  private long state;
  private String storageId;
  private long failed;
  private long capacity;
  private long dfsUsed;
  private long remaining;
  private long blockPoolUsed;

  public DataNodeStorageInfo(String uuid, long sid, long state,
      String storageId, long failed, long capacity,
      long dfsUsed, long remaining, long blockPoolUsed) {
    this.uuid = uuid;
    this.sid = sid;
    this.state = state;
    this.storageId = storageId;
    this.failed = failed;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DataNodeStorageInfo that = (DataNodeStorageInfo) o;

    if (sid != that.sid) return false;
    if (state != that.state) return false;
    if (failed != that.failed) return false;
    if (capacity != that.capacity) return false;
    if (dfsUsed != that.dfsUsed) return false;
    if (remaining != that.remaining) return false;
    if (blockPoolUsed != that.blockPoolUsed) return false;
    if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;
    return storageId != null ? storageId.equals(that.storageId) : that.storageId == null;
  }

  @Override
  public int hashCode() {
    int result = uuid != null ? uuid.hashCode() : 0;
    result = 31 * result + (int) (sid ^ (sid >>> 32));
    result = 31 * result + (int) (state ^ (state >>> 32));
    result = 31 * result + (storageId != null ? storageId.hashCode() : 0);
    result = 31 * result + (int) (failed ^ (failed >>> 32));
    result = 31 * result + (int) (capacity ^ (capacity >>> 32));
    result = 31 * result + (int) (dfsUsed ^ (dfsUsed >>> 32));
    result = 31 * result + (int) (remaining ^ (remaining >>> 32));
    result = 31 * result + (int) (blockPoolUsed ^ (blockPoolUsed >>> 32));
    return result;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public long getSid() {
    return sid;
  }

  public void setSid(long sid) {
    this.sid = sid;
  }

  public long getState() {
    return state;
  }

  public void setState(long state) {
    this.state = state;
  }

  public String getStorageId() {
    return storageId;
  }

  public void setStorageId(String storageId) {
    this.storageId = storageId;
  }

  public long getFailed() {
    return failed;
  }

  public void setFailed(long failed) {
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

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  public void setBlockPoolUsed(long blockPool) {
    this.blockPoolUsed = blockPool;
  }

  @Override
  public String toString() {
    return String.format("DataNodeStorageInfo{uuid=\'%s\', sid=\'%s\', " +
            "state=%d, storage_id=\'%s\', failed=%d, capacity=%d, " +
            "dfs_used=%d, remaining=%d, block_pool_used=%d}",
        uuid, sid, state, storageId, failed,
        capacity, dfsUsed, remaining, blockPoolUsed);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String uuid;
    private long sid;
    private long state;
    private String storageId;
    private long failed;
    private long capacity;
    private long dfsUsed;
    private long remaining;
    private long blockPoolUsed;

    public Builder setUuid(String uuid) {
      this.uuid = uuid;
      return this;
    }
    public Builder setSid(long sid) {
      this.sid = sid;
      return this;
    }
    public Builder setState(long state) {
      this.state = state;
      return this;
    }
    public Builder setStorageId(String storageId) {
      this.storageId = storageId;
      return this;
    }
    public Builder setFailed(long failed) {
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
    public Builder setBlockPoolUsed(long blockPoolUsed) {
      this.blockPoolUsed = blockPoolUsed;
      return this;
    }

    public DataNodeStorageInfo build() {
      return new DataNodeStorageInfo(uuid, sid, state, storageId, failed,
          capacity, dfsUsed, remaining, blockPoolUsed);
    }
  }
}