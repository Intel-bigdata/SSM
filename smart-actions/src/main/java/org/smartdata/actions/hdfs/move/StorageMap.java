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
package org.smartdata.actions.hdfs.move;

import org.apache.hadoop.fs.StorageType;

import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Storage map.
 */
class StorageMap {
  private final Dispatcher.StorageGroupMap<Dispatcher.Source> sources
          = new Dispatcher.StorageGroupMap<>();
  private final Dispatcher.StorageGroupMap<Dispatcher.StorageGroup> targets
          = new Dispatcher.StorageGroupMap<>();
  private final EnumMap<StorageType, List<Dispatcher.StorageGroup>> targetStorageTypeMap
          = new EnumMap<>(StorageType.class);

  StorageMap() {
    for (StorageType t : StorageType.getMovableTypes()) {
      targetStorageTypeMap.put(t, new LinkedList<Dispatcher.StorageGroup>());
    }
  }

  void add(Dispatcher.Source source, Dispatcher.StorageGroup target) {
    sources.put(source);
    if (target != null) {
      targets.put(target);
      getTargetStorages(target.getStorageType()).add(target);
    }
  }

  Dispatcher.Source getSource(MLocation ml) {
    return get(sources, ml);
  }

  Dispatcher.StorageGroupMap<Dispatcher.StorageGroup> getTargets() {
    return targets;
  }

  Dispatcher.StorageGroup getTarget(String uuid, StorageType storageType) {
    return targets.get(uuid, storageType);
  }

  static <G extends Dispatcher.StorageGroup> G get(Dispatcher.StorageGroupMap<G> map, MLocation ml) {
    return map.get(ml.datanode.getDatanodeUuid(), ml.storageType);
  }

  List<Dispatcher.StorageGroup> getTargetStorages(StorageType t) {
    return targetStorageTypeMap.get(t);
  }
}
