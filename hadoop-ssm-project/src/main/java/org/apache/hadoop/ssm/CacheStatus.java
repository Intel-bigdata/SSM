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
package org.apache.hadoop.ssm;

import java.util.List;
import java.util.Map;

/**
 * Created by cc on 17-2-28.
 */
public class CacheStatus {
  // the key named cachePoolName
  private Map<String, List<cacheFileInfo>> cacheStatusMap;
  private long cacheCapacity;
  private long cacheUsed;
  private long cacheRemaining;
  private float cacheUsedPercentage;

  public CacheStatus() {
    cacheCapacity = 0;
    cacheUsed = 0;
    cacheRemaining = 0;
    cacheUsedPercentage = 0;
  }

   class cacheFileInfo {
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

  public Map<String, List<cacheFileInfo>> getCacheStatusMap() {
    return cacheStatusMap;
  }

  public void setCacheStatusMap(Map<String, List<cacheFileInfo>> cacheStatusMap) {
    this.cacheStatusMap = cacheStatusMap;
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
