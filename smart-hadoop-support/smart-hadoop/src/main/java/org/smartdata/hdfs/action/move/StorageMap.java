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
package org.smartdata.hdfs.action.move;

import com.google.common.base.Preconditions;
import org.smartdata.hdfs.CompatibilityHelperLoader;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Storage map.
 */
public class StorageMap {
  private final StorageGroupMap<Source> sources;
  private final StorageGroupMap<StorageGroup> targets;
  private final Map<String, List<StorageGroup>> targetStorageTypeMap;

  public StorageMap() {
    this.sources = new StorageGroupMap<>();
    this.targets = new StorageGroupMap<>();
    this.targetStorageTypeMap = new HashMap<>();
    for (String t : CompatibilityHelperLoader.getHelper().getMovableTypes()) {
      targetStorageTypeMap.put(t, new LinkedList<StorageGroup>());
    }
  }

  public void add(Source source, StorageGroup target) {
    sources.put(source);
    if (target != null) {
      targets.put(target);
      getTargetStorages(target.getStorageType()).add(target);
    }
  }

  public Source getSource(MLocation ml) {
    return get(sources, ml);
  }

  public StorageGroupMap<StorageGroup> getTargets() {
    return targets;
  }

  public StorageGroup getTarget(String uuid, String storageType) {
    return targets.get(uuid, storageType);
  }

  public static <G extends StorageGroup> G get(StorageGroupMap<G> map, MLocation ml) {
    return map.get(ml.datanode.getDatanodeUuid(), ml.storageType);
  }

  public List<StorageGroup> getTargetStorages(String t) {
    return targetStorageTypeMap.get(t);
  }

  public static class StorageGroupMap<G extends StorageGroup> {
    private static String toKey(String datanodeUuid, String storageType) {
      return datanodeUuid + ":" + storageType;
    }

    private final Map<String, G> map = new HashMap<String, G>();

    public G get(String datanodeUuid, String storageType) {
      return map.get(toKey(datanodeUuid, storageType));
    }

    public void put(G g) {
      final String key = toKey(g.getDatanodeInfo().getDatanodeUuid(), g.getStorageType());
      final StorageGroup existing = map.put(key, g);
      Preconditions.checkState(existing == null);
    }

    int size() {
      return map.size();
    }

    void clear() {
      map.clear();
    }

    public Collection<G> values() {
      return map.values();
    }
  }
}
