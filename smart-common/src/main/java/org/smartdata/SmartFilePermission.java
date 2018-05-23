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
