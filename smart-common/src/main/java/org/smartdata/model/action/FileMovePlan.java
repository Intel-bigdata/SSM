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
package org.smartdata.model.action;

import com.google.gson.Gson;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plan of MoverScheduler to indicate block, source and target.
 */
public class FileMovePlan {
  public static final String MAX_CONCURRENT_MOVES = "maxConcurrentMoves";
  public static final String MAX_NUM_RETRIES = "maxNumRetries";
  // info of the namenode
  private URI namenode;

  // info of the file
  private String fileName;
  private boolean isDir;
  private long fileLength;
  private long fileLengthToMove; // length to move in file level
  private long sizeToMove;  // total bytes to move
  private int blocksToMove; // number of file blocks

  // info of source datanode
  private List<String> sourceUuids;
  private List<String> sourceStoragetypes;

  // info of target datanode
  private List<String> targetIpAddrs;
  private List<Integer> targetXferPorts;
  private List<String> targetStorageTypes;

  // info of block
  private List<Long> blockIds;

  private Map<String, String> properties;

  public FileMovePlan(URI namenode, String fileName) {
    this.namenode = namenode;
    this.fileName = fileName;
    sourceUuids = new ArrayList<>();
    sourceStoragetypes = new ArrayList<>();
    targetIpAddrs = new ArrayList<>();
    targetXferPorts = new ArrayList<>();
    targetStorageTypes = new ArrayList<>();
    properties = new HashMap<>();
    blockIds = new ArrayList<>();
  }

  public FileMovePlan() {
    this(null, null);
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void setNamenode(URI namenode) {
    this.namenode = namenode;
  }

  public void addPlan(long blockId, String srcDatanodeUuid, String srcStorageType,
      String dstDataNodeIpAddr, int dstDataNodeXferPort, String dstStorageType) {
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

  public List<String> getSourceStoragetypes() {
    return sourceStoragetypes;
  }

  public List<String> getTargetIpAddrs() {
    return targetIpAddrs;
  }

  public List<Integer> getTargetXferPorts() {
    return targetXferPorts;
  }

  public List<String> getTargetStorageTypes() {
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

  public void addProperty(String property, String value) {
    if (property != null) {
      properties.put(property, value);
    }
  }

  public String getPropertyValue(String property, String defaultValue) {
    if (properties.containsKey(property)) {
      return properties.get(property);
    }
    return defaultValue;
  }

  public int getPropertyValueInt(String property, int defaultValue) {
    String v = getPropertyValue(property, null);
    if (v == null) {
      return defaultValue;
    }
    return Integer.parseInt(v);
  }

  @Override
  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  public static FileMovePlan fromJsonString(String jsonPlan) {
    Gson gson = new Gson();
    return gson.fromJson(jsonPlan, FileMovePlan.class);
  }

  public long getFileLength() {
    return fileLength;
  }

  public void setFileLength(long fileLength) {
    this.fileLength = fileLength;
  }

  public long getSizeToMove() {
    return sizeToMove;
  }

  public void setSizeToMove(long sizeToMove) {
    this.sizeToMove = sizeToMove;
  }

  public void addSizeToMove(long size) {
    this.sizeToMove += size;
  }

  public int getBlocksToMove() {
    return blocksToMove;
  }

  public void incBlocksToMove() {
    this.blocksToMove += 1;
  }

  public long getFileLengthToMove() {
    return fileLengthToMove;
  }

  public void addFileLengthToMove(long fileLengthToMove) {
    this.fileLengthToMove += fileLengthToMove;
  }

  public boolean isDir() {
    return isDir;
  }

  public void setDir(boolean dir) {
    isDir = dir;
  }
}
