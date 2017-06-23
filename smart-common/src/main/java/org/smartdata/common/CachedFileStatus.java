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
package org.smartdata.common;

/**
 * Information maintained for a file cached in hdfs.
 */
public class CachedFileStatus {
  private long fid;
  private String path;
  private long fromTime;
  private long lastAccessTime;
  private int numAccessed;

  public CachedFileStatus() {

  }


  public CachedFileStatus(long fid,
                          String path,
                          long fromTime,
                          long lastAccessTime,
                          int numAccessed) {
    this.fid = fid;
    this.path = path;
    this.fromTime = fromTime;
    this.lastAccessTime = lastAccessTime;
    this.numAccessed = numAccessed;
  }

  public long getFid() {
    return fid;
  }

  public void setFid(long fid) {
    this.fid = fid;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getFromTime() {
    return fromTime;
  }

  public void setFromTime(long fromTime) {
    this.fromTime = fromTime;
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  public int getNumAccessed() {
    return numAccessed;
  }

  public void setNumAccessed(int numAccessed) {
    this.numAccessed = numAccessed;
  }
}
