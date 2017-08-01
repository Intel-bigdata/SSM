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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A processor to do Mover action.
 */
class OldMoverProcessor {
    static final Logger LOG = LoggerFactory.getLogger(MoverProcessor.class);

    private final DFSClient dfs;
    private final Dispatcher dispatcher;
    private final List<Path> targetPaths;
    private final int retryMaxAttempts;
    private final OldStorageMap storages;
    private final AtomicInteger retryCount;
    private final List<String> snapshottableDirs = new ArrayList<String>();

    private final BlockStoragePolicy[] blockStoragePolicies;
    private long movedBlocks = 0;
    private long remainingBlocks = 0;
    private final MoverStatus moverStatus;

    public OldMoverProcessor(Dispatcher dispatcher, List<Path> targetPaths,
                   AtomicInteger retryCount, int retryMaxAttempts, OldStorageMap storages,
                   MoverStatus moverStatus)
            throws IOException {
        this.dispatcher = dispatcher;
        this.dfs = dispatcher.getDistributedFileSystem().getClient();
        this.targetPaths = targetPaths;
        this.retryMaxAttempts = retryMaxAttempts;
        this.storages = storages;
        this.retryCount = retryCount;
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

    private void getSnapshottableDirs() {
        SnapshottableDirectoryStatus[] dirs = null;
        try {
            dirs = dfs.getSnapshottableDirListing();
        } catch (IOException e) {
            LOG.warn("Failed to get snapshottable directories."
                    + " Ignore and continue.", e);
        }
        if (dirs != null) {
            for (SnapshottableDirectoryStatus dir : dirs) {
                snapshottableDirs.add(dir.getFullPath().toString());
            }
        }
    }

    /**
     * @return true if the given path is a snapshot path and the corresponding
     * INode is still in the current fsdirectory.
     */
    private boolean isSnapshotPathInCurrent(String path) throws IOException {
        // if the parent path contains "/.snapshot/", this is a snapshot path
        if (path.contains(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR)) {
            String[] pathComponents = INode.getPathNames(path);
            if (HdfsConstants.DOT_SNAPSHOT_DIR
                    .equals(pathComponents[pathComponents.length - 2])) {
                // this is a path for a specific snapshot (e.g., /foo/.snapshot/s1)
                return false;
            }
            String nonSnapshotPath = convertSnapshotPath(pathComponents);
            return dfs.getFileInfo(nonSnapshotPath) != null;
        } else {
            return false;
        }
    }

    /**
     * convert a snapshot path to non-snapshot path. E.g.,
     * /foo/.snapshot/snapshot-name/bar --> /foo/bar
     */
    private String convertSnapshotPath(String[] pathComponents) {
        StringBuilder sb = new StringBuilder(Path.SEPARATOR);
        for (int i = 0; i < pathComponents.length; i++) {
            if (pathComponents[i].equals(HdfsConstants.DOT_SNAPSHOT_DIR)) {
                i++;
            } else {
                sb.append(pathComponents[i]);
            }
        }
        return sb.toString();
    }

    private Dispatcher.DBlock newDBlock(LocatedBlock lb, List<OldMLocation> locations) {
        Block blk = lb.getBlock().getLocalBlock();
        Dispatcher.DBlock db;
        db = new Dispatcher.DBlock(blk);
        for(OldMLocation ml : locations) {
            Dispatcher.DDatanode.StorageGroup source = storages.getSource(ml);
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
    public ExitStatus processNamespace() throws IOException {
        getSnapshottableDirs();
        MoverProcessResult result = new MoverProcessResult();
        for (Path target : targetPaths) {
            processPath(target.toUri().getPath(), result);
        }
        // wait for pending move to finish and retry the failed migration
        boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.getTargets()
                .values());
        if (hasFailed) {
            if (retryCount.get() == retryMaxAttempts) {
                result.setRetryFailed();
                LOG.error("Failed to move some block's after "
                        + retryMaxAttempts + " retries.");
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
     * @return whether there is still remaining migration work for the next
     * round
     */
    private void processPath(String fullPath, MoverProcessResult result) {
        for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME; ; ) {
            final DirectoryListing children;
            try {
                children = dfs.listPaths(fullPath, lastReturnedName, true);
            } catch (IOException e) {
                LOG.warn("Failed to list directory " + fullPath
                        + ". Ignore the directory and continue.", e);
                return;
            }
            if (children == null) {
                return;
            }
            for (HdfsFileStatus child : children.getPartialListing()) {
                processRecursively(fullPath, child, result);
            }
            if (children.hasMore()) {
                lastReturnedName = children.getLastName();
            } else {
                return;
            }
        }
    }

    /**
     * @return whether the migration requires next round
     */
    private void processRecursively(String parent, HdfsFileStatus status,
                                    MoverProcessResult result) {
        String fullPath = status.getFullName(parent);
        if (status.isDir()) {
            if (!fullPath.endsWith(Path.SEPARATOR)) {
                fullPath = fullPath + Path.SEPARATOR;
            }

            processPath(fullPath, result);
            // process snapshots if this is a snapshottable directory
            if (snapshottableDirs.contains(fullPath)) {
                final String dirSnapshot = fullPath + HdfsConstants.DOT_SNAPSHOT_DIR;
                processPath(dirSnapshot, result);
            }
        } else if (!status.isSymlink()) { // file
            try {
                if (!isSnapshotPathInCurrent(fullPath)) {
                    // the full path is a snapshot path but it is also included in the
                    // current directory tree, thus ignore it.
                    processFile(fullPath, (HdfsLocatedFileStatus) status, result);
                }
            } catch (IOException e) {
                LOG.warn("Failed to check the moverStatus of " + parent
                        + ". Ignore it and continue.", e);
            }
        }
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
        List<StorageType> types = policy.chooseStorageTypes(
                status.getReplication());

        final LocatedBlocks locatedBlocks = status.getBlockLocations();
        final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
        List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
        for (int i = 0; i < lbs.size(); i++) {
            if (i == lbs.size() - 1 && !lastBlkComplete) {
                // last block is incomplete, skip it
                continue;
            }
            LocatedBlock lb = lbs.get(i);
            final StorageTypeDiff diff = new StorageTypeDiff(types,
                    lb.getStorageTypes());
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

    private boolean scheduleMoveBlock(StorageTypeDiff diff, LocatedBlock lb) {
        final List<OldMLocation> locations = OldMLocation.toLocations(lb);
        Collections.shuffle(locations);
        final Dispatcher.DBlock db = newDBlock(lb, locations);

        for (final StorageType t : diff.existing) {
            for (final OldMLocation ml : locations) {
                final Dispatcher.Source source = storages.getSource(ml);
                if (ml.storageType == t && source != null) {
                    // try to schedule one replica move.
                    if (scheduleMoveReplica(db, source, diff.expected)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @VisibleForTesting
    private boolean scheduleMoveReplica(Dispatcher.DBlock db, OldMLocation ml,
                                List<StorageType> targetTypes) {
        final Dispatcher.Source source = storages.getSource(ml);
        return source == null ? false : scheduleMoveReplica(db, source,
                targetTypes);
    }

    private boolean scheduleMoveReplica(Dispatcher.DBlock db, Dispatcher.Source source,
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
    private boolean chooseTargetInSameNode(Dispatcher.DBlock db, Dispatcher.Source source,
                                   List<StorageType> targetTypes) {
        for (StorageType t : targetTypes) {
            Dispatcher.DDatanode.StorageGroup target = storages.getTarget(source.getDatanodeInfo()
                    .getDatanodeUuid(), t);
            if (target == null) {
                continue;
            }
            final Dispatcher.PendingMove pm = source.addPendingMove(db, target);
            if (pm != null) {
                dispatcher.executePendingMove(pm);
                return true;
            }
        }
        return false;
    }

    private boolean chooseTarget(Dispatcher.DBlock db, Dispatcher.Source source,
                         List<StorageType> targetTypes, Matcher matcher) {
        final NetworkTopology cluster = dispatcher.getCluster();
        for (StorageType t : targetTypes) {
            final List<Dispatcher.DDatanode.StorageGroup> targets = storages.getTargetStorages(t);
            Collections.shuffle(targets);
            for (Dispatcher.DDatanode.StorageGroup target : targets) {
                if (matcher.match(cluster, source.getDatanodeInfo(),
                        target.getDatanodeInfo())) {
                    final Dispatcher.PendingMove pm = source.addPendingMove(db, target);
                    if (pm != null) {
                        dispatcher.executePendingMove(pm);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Describe the result for MoverProcessor.
     */
    private class MoverProcessResult {
        private boolean hasRemaining;
        private boolean noBlockMoved;
        private boolean retryFailed;

        public MoverProcessResult() {
            hasRemaining = false;
            noBlockMoved = true;
            retryFailed = false;
        }

        public boolean isHasRemaining() {
            return hasRemaining;
        }

        public boolean isNoBlockMoved() {
            return noBlockMoved;
        }

        public void updateHasRemaining(boolean hasRemaining) {
            this.hasRemaining |= hasRemaining;
        }

        public void setNoBlockMoved(boolean noBlockMoved) {
            this.noBlockMoved = noBlockMoved;
        }

        public void setRetryFailed() {
            this.retryFailed = true;
        }

        /**
         * @return NO_MOVE_PROGRESS if no progress in move after some retry. Return
         *         SUCCESS if all moves are success and there is no remaining move.
         *         Return NO_MOVE_BLOCK if there moves available but all the moves
         *         cannot be scheduled. Otherwise, return IN_PROGRESS since there
         *         must be some remaining moves.
         */
        public ExitStatus getExitStatus() {
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
    private class StorageTypeDiff {
        final List<StorageType> expected;
        final List<StorageType> existing;

        public StorageTypeDiff(List<StorageType> expected, StorageType[] existing) {
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
        public int removeOverlap(boolean ignoreNonMovable) {
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

        public void removeNonMovable(List<StorageType> types) {
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

