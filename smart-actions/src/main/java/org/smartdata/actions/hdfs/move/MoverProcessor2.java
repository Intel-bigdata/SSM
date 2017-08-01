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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
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
import org.smartdata.model.actions.hdfs.MLocation;
import org.smartdata.model.actions.hdfs.Source;
import org.smartdata.model.actions.hdfs.StorageGroup;
import org.smartdata.model.actions.hdfs.StorageMap;

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
class MoverProcessor2 {
  static final Logger LOG = LoggerFactory.getLogger(MoverProcessor2.class);

  private final DFSClient dfs;
  private final Dispatcher dispatcher;
  private Path targetPath;
  private final StorageMap storages;
  private final AtomicInteger retryCount;

  private final BlockStoragePolicy[] blockStoragePolicies;
  private long movedBlocks = 0;
  private long remainingBlocks = 0;
  private final MoverStatus moverStatus;

  MoverProcessor2(Dispatcher dispatcher, Path targetPath, StorageMap storages,
      MoverStatus moverStatus) throws IOException {
    this.dispatcher = dispatcher;
    this.targetPath = targetPath;
    this.dfs = dispatcher.getDistributedFileSystem().getClient();
    this.storages = storages;
    this.retryCount = new AtomicInteger(1);
    this.blockStoragePolicies = new BlockStoragePolicy[1 <<
        BlockStoragePolicySuite.ID_BIT_LENGTH];
    initStoragePolicies();
    this.moverStatus = moverStatus;
  }

  private void initStoragePolicies() throws IOException {
    BlockStoragePolicy[] policies =
         dispatcher.getDistributedFileSystem().getStoragePolicies();

    for (BlockStoragePolicy policy : policies) {
      this.blockStoragePolicies[policy.getId()] = policy;
    }
  }

  DBlock newDBlock(LocatedBlock lb, List<MLocation> locations) {
    Block blk = lb.getBlock().getLocalBlock();
    DBlock db;
    db = new DBlock(blk);
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
  ExitStatus processNamespace() throws IOException {
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
      processFile(targetPath.toUri().getPath(), (HdfsLocatedFileStatus) status, result);
    }

    // wait for pending move to finish and retry the failed migration
    boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.getTargets().values());
    if (hasFailed) {
      if (retryCount.get() == 1) {
        result.setRetryFailed();
        LOG.error("Failed to move some block's after "
            + 1 + " retries.");
        return result.getExitStatus();
      } else {
        retryCount.incrementAndGet();
      }
    } else {
      // Reset retry count if no failure.
      retryCount.set(0);
    }
    movedBlocks = moverStatus.getTotalBlocks() - remainingBlocks;
    moverStatus.setMovedBlocks(movedBlocks);
    result.updateHasRemaining(hasFailed);
    return result.getExitStatus();
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
    List<StorageType> types = policy.chooseStorageTypes(status.getReplication());

    final LocatedBlocks locatedBlocks = status.getBlockLocations();
    final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
    List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
    for (int i = 0; i < lbs.size(); i++) {
      if (i == lbs.size() - 1 && !lastBlkComplete) {
        // last block is incomplete, skip it
        continue;
      }
      LocatedBlock lb = lbs.get(i);
      final StorageTypeDiff diff = new StorageTypeDiff(types, lb.getStorageTypes());
      int remainingReplications = diff.removeOverlap(true);
      moverStatus.increaseTotalSize(lb.getBlockSize() * remainingReplications);
      moverStatus.increaseTotalBlocks(remainingReplications);
      remainingBlocks += remainingReplications;
      if (remainingReplications != 0) {
        if (scheduleMoveBlock(diff, lb)) {
          result.updateHasRemaining(diff.existing.size() > 1
                  && diff.expected.size() > 1);
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

    for (final StorageType t : diff.existing) {
      for (final MLocation ml : locations) {
        final Source source = storages.getSource(ml);
        if (ml.getStorageType() == t && source != null) {
          // try to schedule one replica move.
          if (scheduleMoveReplica(db, source, diff.expected)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  boolean scheduleMoveReplica(DBlock db, Source source,
                              List<StorageType> targetTypes) {
    // Match storage on the same node
    if (chooseTargetInSameNode(db, source, targetTypes)) {
      return true;
    }

    if (dispatcher.getCluster().isNodeGroupAware()) {
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
                                 List<StorageType> targetTypes) {
    for (StorageType t : targetTypes) {
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
      dispatcher.executePendingMove();
      return true;
    }
    return false;
  }

  boolean chooseTarget(DBlock db, Source source,
                       List<StorageType> targetTypes, Matcher matcher) {
    final NetworkTopology cluster = dispatcher.getCluster();
    for (StorageType t : targetTypes) {
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
          dispatcher.executePendingMove();
          return true;
        }
      }
    }
    return false;
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
    final List<StorageType> expected;
    final List<StorageType> existing;

    StorageTypeDiff(List<StorageType> expected, StorageType[] existing) {
      this.expected = new LinkedList<StorageType>(expected);
      this.existing = new LinkedList<StorageType>(Arrays.asList(existing));
    }

    /**
     * Remove the overlap between the expected types and the existing types.
     * @param  ignoreNonMovable ignore non-movable storage types
     *         by removing them from both expected and existing storage type list
     *         to prevent non-movable storage from being moved.
     * @returns the remaining number of replications to move.
     */
    int removeOverlap(boolean ignoreNonMovable) {
      for(Iterator<StorageType> i = existing.iterator(); i.hasNext(); ) {
        final StorageType t = i.next();
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

    void removeNonMovable(List<StorageType> types) {
      for (Iterator<StorageType> i = types.iterator(); i.hasNext(); ) {
        final StorageType t = i.next();
        if (!t.isMovable()) {
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
