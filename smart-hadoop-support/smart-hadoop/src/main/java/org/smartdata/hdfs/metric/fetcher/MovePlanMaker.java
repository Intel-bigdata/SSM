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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.action.move.DBlock;
import org.smartdata.hdfs.action.move.MLocation;
import org.smartdata.hdfs.action.move.Source;
import org.smartdata.hdfs.action.move.StorageGroup;
import org.smartdata.hdfs.action.move.StorageMap;
import org.smartdata.hdfs.scheduler.MovePlanStatistics;
import org.smartdata.model.action.FileMovePlan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A processor to do Mover action.
 */
public class MovePlanMaker {
  static final Logger LOG = LoggerFactory.getLogger(MovePlanMaker.class);

  private final DFSClient dfs;
  private NetworkTopology networkTopology;
  private StorageMap storages;
  private final AtomicInteger retryCount;

  private final BlockStoragePolicy[] blockStoragePolicies;
  private long movedBlocks = 0;
  private final MovePlanStatistics statistics;
  private FileMovePlan schedulePlan;

  public MovePlanMaker(DFSClient dfsClient, StorageMap storages,
      NetworkTopology cluster, MovePlanStatistics statistics) throws IOException {
    this.dfs = dfsClient;
    this.storages = storages;
    this.networkTopology = cluster;
    this.retryCount = new AtomicInteger(1);
    this.blockStoragePolicies = new BlockStoragePolicy[1 <<
        BlockStoragePolicySuite.ID_BIT_LENGTH];
    initStoragePolicies();
    this.statistics = statistics;
  }

  private void initStoragePolicies() throws IOException {
    BlockStoragePolicy[] policies = dfs.getStoragePolicies();

    for (BlockStoragePolicy policy : policies) {
      this.blockStoragePolicies[policy.getId()] = policy;
    }
  }

  private DBlock newDBlock(LocatedBlock lb, List<MLocation> locations) {
    Block blk = lb.getBlock().getLocalBlock();
    DBlock db = new DBlock(blk);
    for(MLocation ml : locations) {
      StorageGroup source = storages.getSource(ml);
      if (source != null) {
        db.addLocation(source);
      }
    }
    return db;
  }

  public synchronized void updateClusterInfo(StorageMap storages, NetworkTopology cluster) {
    this.storages = storages;
    this.networkTopology = cluster;
  }

  /**
   * @return whether there is still remaining migration work for the next
   * round
   */
  public synchronized FileMovePlan processNamespace(Path targetPath) throws IOException {
    schedulePlan = new FileMovePlan();
    String filePath = targetPath.toUri().getPath();
    schedulePlan.setFileName(filePath);

    HdfsFileStatus status = dfs.getFileInfo(filePath);
    if (status == null) {
      throw new IOException("File '" + filePath + "' not found!");
    }
    if (status.isDir()) {
      schedulePlan.setDir(true);
      return schedulePlan;
    }

    DirectoryListing files = dfs.listPaths(filePath, HdfsFileStatus.EMPTY_NAME, true);
    HdfsFileStatus[] statuses = files.getPartialListing();
    if (statuses == null || statuses.length == 0) {
      throw new IOException("File '" + filePath + "' not found!");
    }
    if (statuses.length != 1) {
      throw new IOException("Get '" + filePath + "' file located status error.");
    }
    status = statuses[0];
    if (status.isDir()) {
      throw new IOException("Unexpected '" + filePath + "' directory located status error.");
    }
    schedulePlan.setDir(false);
    schedulePlan.setFileLength(status.getLen());
    processFile(targetPath.toUri().getPath(), (HdfsLocatedFileStatus) status);
    return schedulePlan;
  }

  /**
   * @return true if it is necessary to run another round of migration
   */
  private void processFile(String fullPath, HdfsLocatedFileStatus status) {
    byte policyId = status.getStoragePolicy();
    if (policyId == BlockStoragePolicySuite.ID_UNSPECIFIED) {
      return;
    }
    final BlockStoragePolicy policy = blockStoragePolicies[policyId];
    if (policy == null) {
      LOG.warn("Failed to get the storage policy of file " + fullPath);
      return;
    }
    List<String> types =
        CompatibilityHelperLoader.getHelper().chooseStorageTypes(policy, status.getReplication());

    final LocatedBlocks locatedBlocks = status.getBlockLocations();
    final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
    List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
    for (int i = 0; i < lbs.size(); i++) {
      if (i == lbs.size() - 1 && !lastBlkComplete) {
        // last block is incomplete, skip it
        continue;
      }
      LocatedBlock lb = lbs.get(i);
      final StorageTypeDiff diff =
          new StorageTypeDiff(types, CompatibilityHelperLoader.getHelper().getStorageTypes(lb));
      int remainingReplications = diff.removeOverlap(true);
      long toMove = lb.getBlockSize() * remainingReplications;
      schedulePlan.addSizeToMove(toMove);
      schedulePlan.incBlocksToMove();
      schedulePlan.addFileLengthToMove(lb.getBlockSize());
      statistics.increaseTotalSize(toMove);
      statistics.increaseTotalBlocks(remainingReplications);
      if (remainingReplications != 0) {
        scheduleMoveBlock(diff, lb);
      }
    }
  }

  boolean scheduleMoveBlock(StorageTypeDiff diff, LocatedBlock lb) {
    final List<MLocation> locations = MLocation.toLocations(lb);
    Collections.shuffle(locations);
    final DBlock db = newDBlock(lb, locations);
    boolean needMove = false;

    for (int i = 0; i < diff.existing.size(); i++) {
      String t = diff.existing.get(i);
      MLocation ml = locations.get(i);
      final Source source = storages.getSource(ml);
      if (ml.getStorageType().equals(t) && source != null) {
        // try to schedule one replica move.
        if (scheduleMoveReplica(db, source, Arrays.asList(diff.expected.get(i)))) {
          needMove = true;
        }
      }
    }
    return needMove;
  }

  boolean scheduleMoveReplica(DBlock db, Source source,
                              List<String> targetTypes) {
    // Match storage on the same node
    if (chooseTargetInSameNode(db, source, targetTypes)) {
      return true;
    }

    if (networkTopology.isNodeGroupAware()) {
      if (chooseTarget(db, source, targetTypes, Matcher.SAME_NODE_GROUP)) {
        return true;
      }
    }

    // Then, match nodes on the same rack
    if (chooseTarget(db, source, targetTypes, Matcher.SAME_RACK)) {
      return true;
    }
    // At last, match all remaining nodes
    return chooseTarget(db, source, targetTypes, Matcher.ANY_OTHER);
  }

  /**
   * Choose the target storage within same Datanode if possible.
   */
  boolean chooseTargetInSameNode(DBlock db, Source source,
                                 List<String> targetTypes) {
    for (String t : targetTypes) {
      StorageGroup target = storages.getTarget(source.getDatanodeInfo()
              .getDatanodeUuid(), t);
      if (target == null) {
        continue;
      }
//      final Dispatcher.PendingMove pm = new Dispatcher.PendingMove(source, target);
//      if (pm != null) {
//        dispatcher.executePendingMove(pm);
//        return true;
//      }
//      dispatcher.executePendingMove();
      addPlan(source, target, db.getBlock().getBlockId());
      return true;
    }
    return false;
  }

  boolean chooseTarget(DBlock db, Source source,
                       List<String> targetTypes, Matcher matcher) {
    final NetworkTopology cluster = this.networkTopology;
    for (String t : targetTypes) {
      final List<StorageGroup> targets = storages.getTargetStorages(t);
      Collections.shuffle(targets);
      for (StorageGroup target : targets) {
        if (matcher.match(cluster, source.getDatanodeInfo(),
                target.getDatanodeInfo())) {
//          final Dispatcher.PendingMove pm = source.addPendingMove(db, target);
//          if (pm != null) {
//            dispatcher.executePendingMove(pm);
//            return true;
//          }
//          dispatcher.executePendingMove();
          addPlan(source, target, db.getBlock().getBlockId());
          return true;
        }
      }
    }
    return false;
  }

  private void addPlan(StorageGroup source, StorageGroup target, long blockId) {
    DatanodeInfo sourceDatanode = source.getDatanodeInfo();
    DatanodeInfo targetDatanode = target.getDatanodeInfo();
    schedulePlan.addPlan(blockId, sourceDatanode.getDatanodeUuid(), source.getStorageType(),
        targetDatanode.getIpAddr(), targetDatanode.getXferPort(), target.getStorageType());
  }

  /**
   * Record and process the difference of storage types between source and
   * destination during Mover.
   */
  class StorageTypeDiff {
    final List<String> expected;
    final List<String> existing;

    StorageTypeDiff(List<String> expected, String[] existing) {
      this.expected = new LinkedList<String>(expected);
      this.existing = new LinkedList<String>(Arrays.asList(existing));
    }

    /**
     * Remove the overlap between the expected types and the existing types.
     * @param  ignoreNonMovable ignore non-movable storage types
     *         by removing them from both expected and existing storage type list
     *         to prevent non-movable storage from being moved.
     * @returns the remaining number of replications to move.
     */
    int removeOverlap(boolean ignoreNonMovable) {
      for(Iterator<String> i = existing.iterator(); i.hasNext(); ) {
        final String t = i.next();
        if (expected.remove(t)) {
          i.remove();
        }
      }
      if (ignoreNonMovable) {
        removeNonMovable(existing);
        removeNonMovable(expected);
      }
      return existing.size() < expected.size() ? existing.size() : expected.size();
    }

    void removeNonMovable(List<String> types) {
      for (Iterator<String> i = types.iterator(); i.hasNext(); ) {
        final String t = i.next();
        if (!CompatibilityHelperLoader.getHelper().isMovable(t)) {
          i.remove();
        }
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "{expected=" + expected
              + ", existing=" + existing + "}";
    }
  }
}
