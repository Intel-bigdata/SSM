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
  private String ip;
  private int port;
  private int cacheCapacity;
  private int cacheUsed;
  private String location;

  public DataNodeInfo(String uuid, String hostname, String ip, int port,
      int cacheCapacity, int cacheUsed, String location) {
    this.uuid = uuid;
    this.hostname = hostname;
    this.ip = ip;
    this.port = port;
    this.cacheCapacity = cacheCapacity;
    this.cacheUsed = cacheUsed;
    this.location = location;
  }

  /**getters & setters**/
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

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getCacheCapacity() {
    return cacheCapacity;
  }

  public void setCacheCapacity(int cacheCapacity) {
    this.cacheCapacity = cacheCapacity;
  }

  public int getCacheUsed() {
    return cacheUsed;
  }

  public void setCacheUsed(int cacheUsed) {
    this.cacheUsed = cacheUsed;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Override
  public int hashCode() {
    int result = (int) (uuid != null ? uuid.hashCode() : 0);
    result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
    result = 31 * result + (ip != null ? ip.hashCode() : 0);
    result = 31 * result + (int) (port ^ (port >>> 32));
    result = 31 * result + (int) (cacheCapacity ^ (cacheCapacity >>> 32));
    result = 31 * result + (int) (cacheUsed ^ (cacheUsed >>> 32));
    result = 31 * result + (location != null ? location.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataNodeInfo)) {
      return false;
    }
    DataNodeInfo dataNodeInfo = (DataNodeInfo) o;

    if (getUuid() != dataNodeInfo.getUuid()) {
        return false;
    }
    if (getHostname() != dataNodeInfo.getHostname()) {
        return false;
    }
    if (getIp() != dataNodeInfo.getIp()) {
        return false;
    }
    if (getPort() != dataNodeInfo.getPort()) {
        return false;
    }
    if (getCacheCapacity() != dataNodeInfo.getCacheCapacity()) {
        return false;
    }
    if (getCacheUsed() != dataNodeInfo.getCacheUsed()) {
        return false;
    }
    if (getLocation() != dataNodeInfo.getLocation()) {
        return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("DataNodeInfo{uuid=%s, hostname=%s, " +
         "ip=%s, port=%d, cache_capacity=%d, cache_used=%d, location=%s}",
         uuid, hostname, ip, port, cacheCapacity, cacheUsed, location);
  }

  public static Builder newBuilder() {
        return Builder.create();
    }

  public static class Builder {
    private String uuid;
    private String hostname;
    private String ip;
    private int port;
    private int cacheCapacity;
    private int cacheUsed;
    private String location;

    public static Builder create() {
            return new Builder();
        }

    public Builder setUuid(String uuid) {
      this.uuid = uuid;
      return this;
    }
    public Builder setHostName(String hostname) {
      this.hostname = hostname;
      return this;
    }
    public Builder setIp(String ip) {
      this.ip = ip;
      return this;
    }
    public Builder setPort(int port) {
      this.port = port;
      return this;
    }
    public Builder setCacheCapacity(int cacheCapacity) {
      this.cacheCapacity = cacheCapacity;
      return this;
    }
    public Builder setCacheUsed(int cacheUsed) {
      this.cacheUsed = cacheUsed;
      return this;
    }
    public Builder setLocation(String location) {
      this.location = location;
      return this;
    }
    public DataNodeInfo build() {
      return new DataNodeInfo(uuid, hostname, ip, port,
          cacheCapacity, cacheUsed, location);
    }
  }
}
