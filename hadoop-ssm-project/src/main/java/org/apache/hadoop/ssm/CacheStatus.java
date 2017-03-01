package org.apache.hadoop.ssm;

import java.util.Map;

/**
 * Created by cc on 17-2-28.
 */
public class CacheStatus {
  private String CachePoolName;
  private Map<String, cacheFileInfo> cacheStatusMap;
  private int cacheCapacity;
  private int cacheUsed;
  private int cacheRemaining;
  private int cacheUsedPercentage;


  public CacheStatus() {
    
  }

  class cacheFileInfo {
    private String filePath;
    private int repliNum;

    public cacheFileInfo() {
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

  public String getCachePoolName() {
    return CachePoolName;
  }

  public void setCachePoolName(String cachePoolName) {
    CachePoolName = cachePoolName;
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

  public Map<String, cacheFileInfo> getCacheStatusMap() {
    return cacheStatusMap;
  }

  public void setCacheStatusMap(Map<String, cacheFileInfo> cacheStatusMap) {
    this.cacheStatusMap = cacheStatusMap;
  }
}
