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

public class ClusterInfo {
  private long cid;
  private String name;
  private String url;
  private String confPath;
  private String state;
  private String type;

  public ClusterInfo(long cid, String name, String url, String confPath, String state, String type) {
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ClusterInfo that = (ClusterInfo) o;

    if (cid != that.cid) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;
    if (url != null ? !url.equals(that.url) : that.url != null) return false;
    if (confPath != null ? !confPath.equals(that.confPath) : that.confPath != null) return false;
    if (state != null ? !state.equals(that.state) : that.state != null) return false;
    return type != null ? type.equals(that.type) : that.type == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (cid ^ (cid >>> 32));
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (url != null ? url.hashCode() : 0);
    result = 31 * result + (confPath != null ? confPath.hashCode() : 0);
    result = 31 * result + (state != null ? state.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return String.format("ClusterInfo{cid=%s, name=\'%s\', url=\'%s\', confPath=\'%s\'," +
        " state=\'%s\', type=\'%s\'}", cid, name, url, confPath, state, type);
  }
}
