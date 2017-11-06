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
  private String location;
  private ExecutorType executorType;

  public NodeInfo(String id, String location, ExecutorType executorType) {
    this.id = id;
    this.location = location;
    this.executorType = executorType;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public ExecutorType getExecutorType() {
    return executorType;
  }

  @Override
  public String toString() {
    return String.format("{id=%s, location=%s, executorType=%s}", id, location, executorType);
  }
}

