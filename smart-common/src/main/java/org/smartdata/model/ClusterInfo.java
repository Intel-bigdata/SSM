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

import java.util.Objects;

public class ClusterInfo {
  private long cid;
  private String name;
  private String url;
  private String confPath;
  private String state;
  private String type;

  public ClusterInfo(
      long cid, String name, String url, String confPath, String state, String type) {
    this.cid = cid;
    this.name = name;
    this.url = url;
    this.confPath = confPath;
    this.state = state;
    this.type = type;
  }

  public ClusterInfo() {
  }

  public long getCid() {
    return cid;
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getConfPath() {
    return confPath;
  }

  public void setConfPath(String confPath) {
    this.confPath = confPath;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterInfo that = (ClusterInfo) o;
    return cid == that.cid
        && Objects.equals(name, that.name)
        && Objects.equals(url, that.url)
        && Objects.equals(confPath, that.confPath)
        && Objects.equals(state, that.state)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cid, name, url, confPath, state, type);
  }

  @Override
  public String toString() {
    return String.format(
        "ClusterInfo{cid=%s, name=\'%s\', url=\'%s\', confPath=\'%s\', state=\'%s\', type=\'%s\'}",
        cid, name, url, confPath, state, type);
  }
}
