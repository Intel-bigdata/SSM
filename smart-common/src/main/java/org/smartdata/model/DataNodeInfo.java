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

public class DataNodeInfo {
  private String uuid;
  private String hostname;
  private String rpcAddress;
  private long cacheCapacity;
  private long cacheUsed;
  private String location;

  public DataNodeInfo(String uuid, String hostname, String rpcAddress,
      long cacheCapacity, long cacheUsed, String location) {
    this.uuid = uuid;
    this.hostname = hostname;
    this.rpcAddress = rpcAddress;
    this.cacheCapacity = cacheCapacity;
    this.cacheUsed = cacheUsed;
    this.location = location;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DataNodeInfo that = (DataNodeInfo) o;

    if (cacheCapacity != that.cacheCapacity) return false;
    if (cacheUsed != that.cacheUsed) return false;
    if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;
    if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null) return false;
    if (rpcAddress != null ? !rpcAddress.equals(that.rpcAddress) : that.rpcAddress != null) return false;
    return location != null ? location.equals(that.location) : that.location == null;
  }

  @Override
  public int hashCode() {
    int result = uuid != null ? uuid.hashCode() : 0;
    result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
    result = 31 * result + (rpcAddress != null ? rpcAddress.hashCode() : 0);
    result = 31 * result + (int) (cacheCapacity ^ (cacheCapacity >>> 32));
    result = 31 * result + (int) (cacheUsed ^ (cacheUsed >>> 32));
    result = 31 * result + (location != null ? location.hashCode() : 0);
    return result;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getRpcAddress() {
    return rpcAddress;
  }

  public void setRpcAddress(String ip) {
    this.rpcAddress = rpcAddress;
  }

  public long getCacheCapacity() {
    return cacheCapacity;
  }

  public void setCacheCapacity(long cacheCapacity) {
    this.cacheCapacity = cacheCapacity;
  }

  public long getCacheUsed() {
    return cacheUsed;
  }

  public void setCacheUsed(long cacheUsed) {
    this.cacheUsed = cacheUsed;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Override
  public String toString() {
    return String.format("DataNodeInfo{uuid=\'%s\', hostname=\'%s\', " +
            "rpcAddress=\'%s\', cache_capacity=%d, cache_used=%d, location=\'%s\'}",
        uuid, hostname, rpcAddress, cacheCapacity, cacheUsed, location);
  }

  public static Builder newBuilder() {
    return new Builder() ;
  }

  public static class Builder {
    private String uuid;
    private String hostname;
    private String rpcAddress;
    private long cacheCapacity;
    private long cacheUsed;
    private String location;

    public Builder setUuid(String uuid) {
      this.uuid = uuid;
      return this;
    }
    public Builder setHostName(String hostname) {
      this.hostname = hostname;
      return this;
    }
    public Builder setRpcAddress(String rpcAddress) {
      this.rpcAddress = rpcAddress;
      return this;
    }
    public Builder setCacheCapacity(long cacheCapacity) {
      this.cacheCapacity = cacheCapacity;
      return this;
    }
    public Builder setCacheUsed(long cacheUsed) {
      this.cacheUsed = cacheUsed;
      return this;
    }
    public Builder setLocation(String location) {
      this.location = location;
      return this;
    }
    public DataNodeInfo build() {
      return new DataNodeInfo(uuid, hostname, rpcAddress,
          cacheCapacity, cacheUsed, location);
    }
  }
}