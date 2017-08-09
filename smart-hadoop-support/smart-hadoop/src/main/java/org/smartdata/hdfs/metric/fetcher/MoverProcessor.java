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
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.action.SchedulePlan;
import org.smartdata.hdfs.action.move.DBlock;
import org.smartdata.hdfs.action.move.MLocation;
import org.smartdata.hdfs.action.move.MoverStatus;
import org.smartdata.hdfs.action.move.Source;
import org.smartdata.hdfs.action.move.StorageGroup;
import org.smartdata.hdfs.action.move.StorageMap;

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
public class MoverProcessor {
  static final Logger LOG = LoggerFactory.getLogger(MoverProcessor.class);

  private final DFSClient dfs;
  private NetworkTopology networkTopology;
  private final StorageMap storages;
  private final AtomicInteger retryCount;

  private final BlockStoragePolicy[] blockStoragePolicies;
  private long movedBlocks = 0;
  private long remainingBlocks = 0;
  private final MoverStatus moverStatus;

  private SchedulePlan schedulePlan;


  public MoverProcessor(DFSClient dfsClient, StorageMap storages,
      NetworkTopology cluster, MoverStatus moverStatus) throws IOException {
    this.dfs = dfsClient;
    this.storages = storages;
    this.networkTopology = cluster;
    this.retryCount = new AtomicInteger(1);
    this.blockStoragePolicies = new BlockStoragePolicy[1 <<
        BlockStoragePolicySuite.ID_BIT_LENGTH];
    initStoragePolicies();
    this.moverStatus = moverStatus;
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

  /**
   * @return whether there is still remaining migration work for the next
   * round
   */
  public ExitStatus processNamespace(Path targetPath) throws IOException {
    MoverProcessResult result = new MoverProcessResult();
    DirectoryListing files = dfs.listPaths(targetPath.toUri().getPath(),
      HdfsFileStatus.EMPTY_NAME, true);
    HdfsFileStatus status = null;
    for (HdfsFileStatus file : files.getPartialListing()) {
      if (!file.isDir()) {
        status = file;
        break;
      }
    }
    if (!status.isSymlink()) { // file
      schedulePlan = new SchedulePlan();
      schedulePlan.setFileName(targetPath.toUri().getPath());
      processFile(targetPath.toUri().getPath(), (HdfsLocatedFileStatus) status, result);
    }

//    // wait for pending move to finish and retry the failed migration
//    boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.getTargets().values());
//    if (hasFailed) {
//      if (retryCount.get() == 1) {
//        result.setRetryFailed();
//        LOG.error("Failed to move some block's after "
//            + 1 + " retries.");
//        return result.getExitStatus();
//      } else {
//        retryCount.incrementAndGet();
//      }
//    } else {
//      // Reset retry count if no failure.
//      retryCount.set(0);
//    }
//    movedBlocks = moverStatus.getTotalBlocks() - remainingBlocks;
//    moverStatus.setMovedBlocks(movedBlocks);
//    result.updateHasRemaining(hasFailed);
    return result.getExitStatus();
  }

  public SchedulePlan getSchedulePlan() {
    return schedulePlan;
  }

  /**
   * @return true if it is necessary to run another round of migration
   */
  private void processFile(String fullPath, HdfsLocatedFileStatus status,
                           MoverProcessResult result) {
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
      moverStatus.increaseTotalSize(lb.getBlockSize() * remainingReplications);
      moverStatus.increaseTotalBlocks(remainingReplications);
      remainingBlocks += remainingReplications;
      if (remainingReplications != 0) {
        if (scheduleMoveBlock(diff, lb)) {
          result.updateHasRemaining(false);
          // One block scheduled successfully, set noBlockMoved to false
          result.setNoBlockMoved(false);
        } else {
          result.updateHasRemaining(true);
        }
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
   * Describe the result for MoverProcessor.
   */
  class MoverProcessResult {
    private boolean hasRemaining;
    private boolean noBlockMoved;
    private boolean retryFailed;

    MoverProcessResult() {
      hasRemaining = false;
      noBlockMoved = true;
      retryFailed = false;
    }

    boolean isHasRemaining() {
      return hasRemaining;
    }

    boolean isNoBlockMoved() {
      return noBlockMoved;
    }

    void updateHasRemaining(boolean hasRemaining) {
      this.hasRemaining |= hasRemaining;
    }

    void setNoBlockMoved(boolean noBlockMoved) {
      this.noBlockMoved = noBlockMoved;
    }

    void setRetryFailed() {
      this.retryFailed = true;
    }

    /**
     * @return NO_MOVE_PROGRESS if no progress in move after some retry. Return
     *         SUCCESS if all moves are success and there is no remaining move.
     *         Return NO_MOVE_BLOCK if there moves available but all the moves
     *         cannot be scheduled. Otherwise, return IN_PROGRESS since there
     *         must be some remaining moves.
     */
    ExitStatus getExitStatus() {
      if (retryFailed) {
        return ExitStatus.NO_MOVE_PROGRESS;
      } else {
        return !isHasRemaining() ? ExitStatus.SUCCESS
                : isNoBlockMoved() ? ExitStatus.NO_MOVE_BLOCK
                : ExitStatus.IN_PROGRESS;
      }
    }
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
