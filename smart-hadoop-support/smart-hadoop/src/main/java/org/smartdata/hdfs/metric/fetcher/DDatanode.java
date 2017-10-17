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
package org.smartdata.hdfs.metric.fetcher;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.smartdata.hdfs.action.move.Source;
import org.smartdata.hdfs.action.move.StorageGroup;

import java.util.HashMap;
import java.util.Map;

/** A class that keeps track of a datanode. */
public class DDatanode {
  final DatanodeInfo datanode;
  private final Map<String, Source> sourceMap;
  private final Map<String, StorageGroup> targetMap;

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + datanode;
  }

  public DDatanode(DatanodeInfo datanode, int maxConcurrentMoves) {
    this.datanode = datanode;
    this.sourceMap = new HashMap<>();
    this.targetMap = new HashMap<>();
  }

  public DatanodeInfo getDatanodeInfo() {
    return datanode;
  }

  private static <G extends StorageGroup> void put(String storageType,
      G g, Map<String, G> map) {
    final StorageGroup existing = map.put(storageType, g);
    Preconditions.checkState(existing == null);
  }

  public StorageGroup addTarget(String storageType) {
    final StorageGroup g = new StorageGroup(this.datanode, storageType);
    put(storageType, g, targetMap);
    return g;
  }

  public Source addSource(String storageType) {
    final Source s = new Source(storageType, this.getDatanodeInfo());
    put(storageType, s, sourceMap);
    return s;
  }
}