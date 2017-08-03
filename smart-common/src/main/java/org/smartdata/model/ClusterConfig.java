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
package org.smartdata.model;

public class ClusterConfig {
  private long cid;
  private String node_name;
  private String config_path;

  public ClusterConfig(int cid, String node_name, String config_path) {
    this.cid = cid;
    this.node_name = node_name;
    this.config_path = config_path;
  }

  public ClusterConfig() {
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public void setNode_name(String node_name) {
    this.node_name = node_name;
  }

  public void setConfig_path(String config_path) {
    this.config_path = config_path;
  }

  public long getCid() {
    return cid;
  }

  public String getNode_name() {
    return node_name;
  }

  public String getConfig_path() {
    return config_path;
  }

  @Override
  public String toString() {
    return String.format("ClusterConfig{cid=%s, node_name=\'%s\', config_path=\'%s\'}",cid,node_name,config_path);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ClusterConfig that = (ClusterConfig) o;

    if (cid != that.cid) return false;
    if (node_name != null ? !node_name.equals(that.node_name) : that.node_name != null) return false;
    return config_path != null ? config_path.equals(that.config_path) : that.config_path == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (cid ^ (cid >>> 32));
    result = 31 * result + (node_name != null ? node_name.hashCode() : 0);
    result = 31 * result + (config_path != null ? config_path.hashCode() : 0);
    return result;
  }
}
