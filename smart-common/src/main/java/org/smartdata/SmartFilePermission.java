package org.smartdata;

import org.smartdata.model.FileInfo;

/**
 * Handling file permission info conveniently.
 */
public class SmartFilePermission {
  private short permission;
  private String owner;
  private String group;

  public SmartFilePermission(FileInfo fileInfo) {
    this.permission = fileInfo.getPermission();
    this.owner = fileInfo.getOwner();
    this.group = fileInfo.getGroup();
  }

  public String getOwner() {
    return owner;
  }

  public String getGroup() {
    return group;
  }

  public short getPermission() {
    return permission;
  }

  @Override
  public int hashCode() {
    return permission ^ owner.hashCode() ^ group.hashCode();
  }

  @Override
  public boolean equals(Object filePermission) {
    if (this == filePermission) {
      return true;
    }
    if (filePermission instanceof SmartFilePermission) {
      SmartFilePermission anPermissionInfo = (SmartFilePermission) filePermission;
      return ((this.permission == anPermissionInfo.permission))
          && this.owner.equals(anPermissionInfo.owner)
          && this.group.equals(anPermissionInfo.group);
    }
    return false;
  }
}
