/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.smart.command.actions;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class CacheStatus {
  // the key named cachePoolName
  private Map<String, List<cacheFileInfo>> cacheStatusMap;
  //cache for each node information,the key means Datanote host name
  private Map<String, nodeCacheInfo> dnCacheStatusMap;
  private long cacheCapacityTotal;
  private long cacheUsedTotal;
  private long cacheRemainingTotal;
  private float cacheUsedPercentageTotal;


  public CacheStatus() {
    cacheCapacityTotal = 0;
    cacheUsedTotal = 0;
    cacheRemainingTotal = 0;
    cacheUsedPercentageTotal = 0;
  }

  public class cacheFileInfo {
    private String filePath;
    private int repliNum;

    public cacheFileInfo() {
      filePath = null;
      repliNum = 0;
    }

    public String getFilePath() {
      return filePath;
    }

    public void setFilePath(String filePath) {
      this.filePath = filePath;
    }

    public int getRepliNum() {
      return repliNum;
    }

    public void setRepliNum(int repliNum) {
      this.repliNum = repliNum;
    }
  }

  //only contain DataNode info
  public static class nodeCacheInfo {
    private long cacheCapacity;
    private long cacheUsed;
    private long cacheRemaining;
    private float cacheUsedPercentage;


    public nodeCacheInfo() {
      cacheCapacity = 0;
      cacheUsed = 0;
      cacheRemaining = 0;
      cacheUsedPercentage = 0;
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

    public long getCacheRemaining() {
      return cacheRemaining;
    }

    public void setCacheRemaining(long cacheRemaining) {
      this.cacheRemaining = cacheRemaining;
    }

    public float getCacheUsedPercentage() {
      return cacheUsedPercentage;
    }

    public void setCacheUsedPercentage(float cacheUsedPercentage) {
      this.cacheUsedPercentage = cacheUsedPercentage;
    }
  }

  public Map<String, List<cacheFileInfo>> getCacheStatusMap() {
    return cacheStatusMap;
  }

  public void setCacheStatusMap(Map<String, List<cacheFileInfo>> cacheStatusMap) {
    this.cacheStatusMap = cacheStatusMap;
  }

  public Map<String, nodeCacheInfo> getdnCacheStatusMap() {
    return dnCacheStatusMap;
  }

  public void setdnCacheStatusMap(Map<String, nodeCacheInfo> dnCacheStatusMap) {
    this.dnCacheStatusMap = dnCacheStatusMap;
  }

  public long getCacheCapacityTotal() {
    return cacheCapacityTotal;
  }

  public void setCacheCapacityTotal(long cacheCapacityTotal) {
    this.cacheCapacityTotal = cacheCapacityTotal;
  }

  public long getCacheUsedTotal() {
    return cacheUsedTotal;
  }

  public void setCacheUsedTotal(long cacheUsedTotal) {
    this.cacheUsedTotal = cacheUsedTotal;
  }

  public long getCacheRemainingTotal() {
    return cacheRemainingTotal;
  }

  public void setCacheRemainingTotal(long cacheRemainingTotal) {
    this.cacheRemainingTotal = cacheRemainingTotal;
  }

  public float getCacheUsedPercentageTotal() {
    return cacheUsedPercentageTotal;
  }

  public void setCacheUsedPercentageTotal(float cacheUsedPercentageTotal) {
    this.cacheUsedPercentageTotal = cacheUsedPercentageTotal;
  }
}
