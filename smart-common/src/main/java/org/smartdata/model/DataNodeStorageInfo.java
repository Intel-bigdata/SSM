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

  public DataNodeStorageInfo(String uuid, String storageType, long state,
      String storageId, long failed, long capacity,
      long dfsUsed, long remaining, long blockPoolUsed) {
    this.uuid = uuid;

    if (storageType.equals("ram")){
      this.sid = 0;
    }

    if (storageType.equals("ssd")){
      this.sid = 1;
    }

    if (storageType.equals("disk")){
      this.sid = 2;
    }

    if (storageType.equals("archive")) {
      this.sid = 3;
    }

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
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataNodeStorageInfo that = (DataNodeStorageInfo) o;
    return sid == that.sid
        && state == that.state
        && failed == that.failed
        && capacity == that.capacity
        && dfsUsed == that.dfsUsed
        && remaining == that.remaining
        && blockPoolUsed == that.blockPoolUsed
        && Objects.equals(uuid, that.uuid)
        && Objects.equals(storageId, that.storageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        uuid, sid, state, storageId, failed, capacity, dfsUsed, remaining, blockPoolUsed);
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

  public void setSid(String storageType) {
    if (storageType.equals("ram")){
      this.sid = 0;
    }

    if (storageType.equals("ssd")){
      this.sid = 1;
    }

    if (storageType.equals("disk")){
      this.sid = 2;
    }

    if (storageType.equals("archive")) {
      this.sid = 3;
    }
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
    return String.format(
        "DataNodeStorageInfo{uuid=\'%s\', sid=\'%s\', "
            + "state=%d, storage_id=\'%s\', failed=%d, capacity=%d, "
            + "dfs_used=%d, remaining=%d, block_pool_used=%d}",
        uuid, sid, state, storageId, failed, capacity, dfsUsed, remaining, blockPoolUsed);
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
