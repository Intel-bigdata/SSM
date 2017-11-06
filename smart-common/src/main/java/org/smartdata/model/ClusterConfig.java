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

import java.util.Objects;

public class ClusterConfig {
  private long cid;
  private String nodeName;
  private String configPath;

  public ClusterConfig(int cid, String nodeName, String configPath) {
    this.cid = cid;
    this.nodeName = nodeName;
    this.configPath = configPath;
  }

  public ClusterConfig() {
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public void setConfig_path(String configPath) {
    this.configPath = configPath;
  }

  public long getCid() {
    return cid;
  }

  public String getNodeName() {
    return nodeName;
  }

  public String getConfigPath() {
    return configPath;
  }

  @Override
  public String toString() {
    return String.format(
        "ClusterConfig{cid=%s, nodeName=\'%s\', configPath=\'%s\'}", cid, nodeName, configPath);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterConfig that = (ClusterConfig) o;
    return cid == that.cid
        && Objects.equals(nodeName, that.nodeName)
        && Objects.equals(configPath, that.configPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cid, nodeName, configPath);
  }
}
