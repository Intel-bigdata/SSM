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
package org.apache.hadoop.hdfs.protocol;

import java.util.List;
import java.util.Map;

public class FilesAccessInfo {
  private long startTime;  // NN local time for statistic
  private long endTime;

  private Map<String, Integer> accessCountMap;
  private List<NNEvent> nnEvents;  // Keep it for now

  public FilesAccessInfo() {}

  public FilesAccessInfo(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public Map<String, Integer> getAccessCountMap() {
    return accessCountMap;
  }

  public void setAccessCountMap(Map<String, Integer> accessCountMap) {
    this.accessCountMap = accessCountMap;
  }

  public void setNnEvents(List<NNEvent> events) {
    this.nnEvents = events;
  }

  public List<NNEvent> getNnEvents() {
    return nnEvents;
  }
}
