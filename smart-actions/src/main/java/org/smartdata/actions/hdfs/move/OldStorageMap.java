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
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;

import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Storage map.
 */
class OldStorageMap {
    private final Dispatcher.StorageGroupMap<Dispatcher.Source> sources
            = new Dispatcher.StorageGroupMap<Dispatcher.Source>();
    private final Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup> targets
            = new Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup>();
    private final EnumMap<StorageType, List<Dispatcher.DDatanode.StorageGroup>> targetStorageTypeMap
            = new EnumMap<StorageType, List<Dispatcher.DDatanode.StorageGroup>>(StorageType.class);

    public OldStorageMap() {
        for (StorageType t : StorageType.getMovableTypes()) {
            targetStorageTypeMap.put(t, new LinkedList<Dispatcher.DDatanode.StorageGroup>());
        }
    }

    public void add(Dispatcher.Source source, Dispatcher.DDatanode.StorageGroup target) {
        sources.put(source);
        if (target != null) {
            targets.put(target);
            getTargetStorages(target.getStorageType()).add(target);
        }
    }

    public Dispatcher.Source getSource(OldMLocation ml) {
        return get(sources, ml);
    }

    public Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup> getTargets() {
        return targets;
    }

    public Dispatcher.DDatanode.StorageGroup getTarget(String uuid, StorageType storageType) {
        return targets.get(uuid, storageType);
    }

    public static <G extends Dispatcher.DDatanode.StorageGroup> G get(Dispatcher.StorageGroupMap<G> map, OldMLocation ml) {
        return map.get(ml.datanode.getDatanodeUuid(), ml.storageType);
    }

    public List<Dispatcher.DDatanode.StorageGroup> getTargetStorages(StorageType t) {
        return targetStorageTypeMap.get(t);
    }
}

