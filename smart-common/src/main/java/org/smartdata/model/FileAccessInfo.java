/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.smartdata.model;

public class FileAccessInfo {
  private Long fid;
  private String path;
  private Integer accessCount;

  public FileAccessInfo(Long fid, String path, Integer accessCount) {
    this.fid = fid;
    this.path = path;
    this.accessCount = accessCount;
  }

  public Long getFid() {
    return fid;
  }

  public void setFid(Long fid) {
    this.fid = fid;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Integer getAccessCount() {
    return accessCount;
  }

  public void setAccessCount(Integer accessCount) {
    this.accessCount = accessCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FileAccessInfo that = (FileAccessInfo) o;

    if (fid != null ? !fid.equals(that.fid) : that.fid != null) return false;
    if (path != null ? !path.equals(that.path) : that.path != null) return false;
    return accessCount != null ? accessCount.equals(that.accessCount) : that.accessCount == null;
  }

  @Override
  public int hashCode() {
    int result = fid != null ? fid.hashCode() : 0;
    result = 31 * result + (path != null ? path.hashCode() : 0);
    result = 31 * result + (accessCount != null ? accessCount.hashCode() : 0);
    return result;
  }
}
