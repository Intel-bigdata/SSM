package org.apache.hadoop.ssm;

import java.util.List;
import java.util.Map;

/**
 * Created by cc on 17-2-28.
 */
public class CacheStatus {
  // the key named cachePoolName
  private Map<String, List<cacheFileInfo>> cacheStatusMap;
  private int cacheCapacity;
  private int cacheUsed;
  private int cacheRemaining;
  private int cacheUsedPercentage;

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

  public int getCacheRemaining() {
    return cacheRemaining;
  }

  public void setCacheRemaining(int cacheRemaining) {
    this.cacheRemaining = cacheRemaining;
  }

  public int getCacheUsedPercentage() {
    return cacheUsedPercentage;
  }

  public void setCacheUsedPercentage(int cacheUsedPercentage) {
    this.cacheUsedPercentage = cacheUsedPercentage;
  }

  public Map<String, List<cacheFileInfo>> getCacheStatusMap() {
    return cacheStatusMap;
  }

  public void setCacheStatusMap(Map<String, List<cacheFileInfo>> cacheStatusMap) {
    this.cacheStatusMap = cacheStatusMap;
  }
}
