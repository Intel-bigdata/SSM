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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mover tool for SSM.
 */
public class OldMover {
    static final Logger LOG = LoggerFactory.getLogger(Mover.class);

    static final String MOVER_ID_PATH = "/system/move.id";

    final Dispatcher dispatcher;
    final OldStorageMap storages;
    final List<Path> targetPaths;
    final int retryMaxAttempts;
    final AtomicInteger retryCount;
    private final MoverStatus status;

    public OldMover(NameNodeConnector nnc, Configuration conf, AtomicInteger retryCount,
          MoverStatus status) {
        final long movedWinWidth = conf.getLong(
                DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY,
                DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_DEFAULT);
        final int moverThreads = conf.getInt(
                DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
                DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
        final int maxConcurrentMovesPerNode = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
                DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
        this.retryMaxAttempts = conf.getInt(
                DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY,
                DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
        this.retryCount = retryCount;
        this.dispatcher = new Dispatcher(nnc, Collections.<String> emptySet(),
                Collections.<String> emptySet(), movedWinWidth, moverThreads, 0,
                maxConcurrentMovesPerNode, conf);
        this.storages = new OldStorageMap();
        this.targetPaths = nnc.getTargetPaths();
        this.status = status;
    }

    @VisibleForTesting
    public void init() throws IOException {
        final List<DatanodeStorageReport> reports = dispatcher.init();
        for(DatanodeStorageReport r : reports) {
            final Dispatcher.DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
            for(StorageType t : StorageType.getMovableTypes()) {
                final Dispatcher.Source source = dn.addSource(t, Long.MAX_VALUE, dispatcher);
                final long maxRemaining = getMaxRemaining(r, t);
                final Dispatcher.DDatanode.StorageGroup target = maxRemaining > 0L ? dn.addTarget(t,
                        maxRemaining) : null;
                storages.add(source, target);
            }
        }
    }

    private ExitStatus run() {
        try {
            init();
            return new OldMoverProcessor(dispatcher, targetPaths, retryCount,
                    retryMaxAttempts, storages, status).processNamespace();
        } catch (IllegalArgumentException e) {
            LOG.info(e + ".  Exiting ...");
            return ExitStatus.ILLEGAL_ARGUMENTS;
        } catch (IOException e) {
            LOG.info(e + ".  Exiting ...");
            return ExitStatus.IO_EXCEPTION;
        } finally {
            dispatcher.shutdownNow();
        }
    }

    private long getMaxRemaining(DatanodeStorageReport report, StorageType t) {
        long max = 0L;
        for(StorageReport r : report.getStorageReports()) {
            if (r.getStorage().getStorageType() == t) {
                if (r.getRemaining() > max) {
                    max = r.getRemaining();
                }
            }
        }
        return max;
    }

    public static int run(Map<URI, List<Path>> namenodes, Configuration conf,
                   MoverStatus status) throws IOException, InterruptedException {
        final long sleeptime =
                conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000 +
                        conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
                                DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000;
        AtomicInteger retryCount = new AtomicInteger(0);

        LOG.info("namenodes = " + namenodes);

        List<NameNodeConnector> connectors = Collections.emptyList();
        try {
            Path moverIdPath = new Path(MOVER_ID_PATH + UUID.randomUUID().toString());
            connectors = NameNodeConnector.newNameNodeConnectors(namenodes,
                    Mover.class.getSimpleName(), moverIdPath, conf,
                    NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);

            while (connectors.size() > 0) {
                Collections.shuffle(connectors);
                Iterator<NameNodeConnector> iter = connectors.iterator();
                while (iter.hasNext()) {
                    NameNodeConnector nnc = iter.next();
                    final OldMover m = new OldMover(nnc, conf, retryCount, status);
                    final ExitStatus r = m.run();

                    if (r == ExitStatus.SUCCESS) {
                        status.setMovedBlocks(status.getTotalBlocks());
                        IOUtils.cleanup(null, nnc);
                        iter.remove();
                    } else if (r != ExitStatus.IN_PROGRESS) {
                        if (r == ExitStatus.NO_MOVE_PROGRESS) {
                            LOG.error("Failed to move some blocks after "
                                    + m.retryMaxAttempts + " retries. Exiting...");
                        } else if (r == ExitStatus.NO_MOVE_BLOCK) {
                            LOG.error("Some blocks can't be moved. Exiting...");
                        } else {
                            LOG.error("Mover failed. Exiting with status " + r
                                    + "... ");
                        }
                        // must be an error statue, return
                        return r.getExitCode();
                    }
                }
                // total values of status are completely set after the first round
                status.completeTotalValueSet();
                Thread.sleep(sleeptime);
            }
            LOG.info("Mover Successful: all blocks satisfy"
                    + " the specified storage policy. Exiting...");
            return ExitStatus.SUCCESS.getExitCode();
        } finally {
            for (NameNodeConnector nnc : connectors) {
                IOUtils.cleanup(null, nnc);
            }
        }
    }

}

