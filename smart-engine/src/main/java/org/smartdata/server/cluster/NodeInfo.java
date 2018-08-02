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
package org.smartdata.server.cluster;


import org.smartdata.model.ExecutorType;

/**
 * Represent each nodes that SSM services (SmartServers and SmartAgents) running on.
 *
 */
public class NodeInfo {
  private String id;
  private String host;
  private int port;
  private ExecutorType executorType;

  public NodeInfo(String id, String location, ExecutorType executorType) {
    this.id = id;
    this.executorType = executorType;
    doSetLocation(location);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getLocation() {
    return host + ":" + port;
  }

  public void setLocation(String location) {
    doSetLocation(location);
  }

  private void doSetLocation(String location) {
    host = null;
    port = 0;
    if (location != null) {
      String[] its = location.split(":");
      if (its.length > 1) {
        port = Integer.valueOf(its[1]);
      }
      host = its[0];
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public ExecutorType getExecutorType() {
    return executorType;
  }

  @Override
  public String toString() {
    return String.format("{id=%s, location=%s, executorType=%s}", id, getLocation(), executorType);
  }
}

