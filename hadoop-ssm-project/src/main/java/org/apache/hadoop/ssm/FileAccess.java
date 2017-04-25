package org.apache.hadoop.ssm;

/**
 * Created by root on 11/4/16.
 */
class FileAccess implements Comparable<FileAccess>{
  private String fileName;
  private Integer accessCount;
  private Long createTime;
  private Boolean isOnSSD;
  private Boolean isOnArchive;
  private Boolean isOnCache;

  public FileAccess(String fileName, Integer accessCount) {
    this(fileName, accessCount, null, false, false, false);
  }

  public FileAccess(String fileName, Long createTime) { this(fileName, 0, createTime, false, false, false);}

  public FileAccess(String fileName, Integer accessCount, Long createTime) {
    this(fileName, accessCount, createTime, false, false, false);
  }

  public FileAccess(String fileName, Integer accessCount, Long createTime,
                    Boolean isOnSSD, Boolean isOnArchive, Boolean isOnCache) {
    this.fileName = fileName;
    this.accessCount = accessCount;
    this.createTime = createTime;
    this.isOnSSD = isOnSSD;
    this.isOnArchive = isOnArchive;
    this.isOnCache = isOnCache;
  }

  public FileAccess(FileAccess fileAccess) {
    this.fileName = fileAccess.fileName;
    this.accessCount = fileAccess.accessCount;
    this.createTime = fileAccess.createTime;
    this.isOnArchive = fileAccess.isOnArchive;
    this.isOnSSD = fileAccess.isOnSSD;
    this.isOnCache = fileAccess.isOnCache;
  }

  public String getFileName() { return fileName;}

  public void setFileName(String fileName) { this.fileName = fileName;}

  public Long getCreateTime() { return createTime;}

  public void setCreateTime(Long createTime) { this.createTime = createTime;}

  public Integer getAccessCount() { return accessCount;}

  public void setAccessCount(Integer accessCount) { this.accessCount = accessCount;}

  public Boolean isOnSSD() { return isOnSSD;}

  public void setSSD() { this.isOnSSD = true;}

  public void unsetSSD() { this.isOnSSD = false;}

  public Boolean isOnArchive() { return this.isOnArchive;}

  public void setArchive() { this.isOnArchive = true;}

  public void unsetArchive() { this.isOnArchive = false;}

  public Boolean isOnCache() { return this.isOnCache;}

  public void setCache() { this.isOnCache = true;}

  public void unsetCache() { this.isOnCache = false;}

  public void increaseAccessCount(Integer cnt) { this.accessCount += cnt;}

  public void decreaseAccessCount(Integer cnt) { this.accessCount -= cnt;}

  public int compareTo(FileAccess other) {
    int result;
    result = this.accessCount.compareTo(other.accessCount);
    if (result == 0) {
      result = this.fileName.compareTo(other.fileName);
    }
    return result;
  }
}
