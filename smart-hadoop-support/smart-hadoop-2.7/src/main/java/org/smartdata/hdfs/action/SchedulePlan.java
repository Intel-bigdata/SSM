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
package org.smartdata.hdfs.action;

import com.google.gson.Gson;
import org.apache.hadoop.fs.StorageType;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Plan of MoverScheduler to indicate block, source and target.
 */
public class SchedulePlan {
  // info of the namenode
  private URI namenode;

  // info of the file
  private String fileName;

  // info of source datanode
  private List<String> sourceUuids;
  private List<StorageType> sourceStoragetypes;

  // info of target datanode
  private List<String> targetIpAddrs;
  private List<Integer> targetXferPorts;
  private List<StorageType> targetStorageTypes;

  // info of block
  private List<Long> blockIds;

  public SchedulePlan(URI namenode, String fileName) {
    this.namenode = namenode;
    this.fileName = fileName;
    sourceUuids = new ArrayList<>();
    sourceStoragetypes = new ArrayList<>();
    targetIpAddrs = new ArrayList<>();
    targetXferPorts = new ArrayList<>();
    targetStorageTypes = new ArrayList<>();
    blockIds = new ArrayList<>();
  }

  public SchedulePlan() {
    this(null, null);
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void setNamenode(URI namenode) {
    this.namenode = namenode;
  }

  public void addPlan(long blockId, String srcDatanodeUuid, StorageType srcStorageType,
      String dstDataNodeIpAddr, int dstDataNodeXferPort, StorageType dstStorageType) {
    blockIds.add(blockId);
    sourceUuids.add(srcDatanodeUuid);
    sourceStoragetypes.add(srcStorageType);
    targetIpAddrs.add(dstDataNodeIpAddr);
    targetXferPorts.add(dstDataNodeXferPort);
    targetStorageTypes.add(dstStorageType);
  }

  public List<String> getSourceUuids() {
    return sourceUuids;
  }

  public List<StorageType> getSourceStoragetypes() {
    return sourceStoragetypes;
  }

  public List<String> getTargetIpAddrs() {
    return targetIpAddrs;
  }

  public List<Integer> getTargetXferPorts() {
    return targetXferPorts;
  }

  public List<StorageType> getTargetStorageTypes() {
    return targetStorageTypes;
  }

  public List<Long> getBlockIds() {
    return blockIds;
  }

  public String getFileName() {
    return fileName;
  }

  public URI getNamenode() {
    return namenode;
  }

  @Override
  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  public static SchedulePlan fromJsonString(String jsonPlan) {
    Gson gson = new Gson();
    return gson.fromJson(jsonPlan, SchedulePlan.class);
  }
}
