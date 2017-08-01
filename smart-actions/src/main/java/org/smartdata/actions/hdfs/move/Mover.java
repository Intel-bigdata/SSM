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
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.actions.hdfs.Source;
import org.smartdata.model.actions.hdfs.StorageGroup;
import org.smartdata.model.actions.hdfs.StorageMap;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mover tool for SSM.
 */
//TODO: Mover will be separated into scheduler and executor, so this class will be abandoned
public class Mover {
  static final Logger LOG = LoggerFactory.getLogger(Mover.class);

  final Dispatcher dispatcher;
  final StorageMap storages;
  final Path targetPath;
  final Configuration conf;

  private final MoverStatus status;

  Mover(NameNodeConnector nnc, Path targetPath, Configuration conf, MoverStatus status) {
    this.conf = conf;
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads = conf.getInt(
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);

    this.dispatcher = new Dispatcher(nnc, movedWinWidth, moverThreads, 0,
        maxConcurrentMovesPerNode, conf);
    this.storages = new StorageMap();
    this.targetPath = targetPath;
    this.status = status;
  }

  @VisibleForTesting
  void init() throws IOException {
    final List<DatanodeStorageReport> reports = dispatcher.init();
    for(DatanodeStorageReport r : reports) {
      final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      for(StorageType t : StorageType.getMovableTypes()) {
        final Source source = dn.addSource(t);
        final long maxRemaining = getMaxRemaining(r, t);
        final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t) : null;
        storages.add(source, target);
      }
    }
  }

  ExitStatus run() throws Exception {
    try {
      init();
      AtomicInteger count = new AtomicInteger(0);
      final long sleeptime =
          conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
              DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000 +
              conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
                  DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000;
      while (true) {
        MoverScheduler mp = new MoverScheduler(dispatcher, targetPath, storages,
            count, 3, status);
        ExitStatus r = mp.processNamespace();
        if (r == ExitStatus.SUCCESS) {
          LOG.info("Mover Successful: all blocks satisfy"
              + " the specified storage policy. Exiting...");
          return r;
        } else if (r != ExitStatus.IN_PROGRESS) {
          if (r == ExitStatus.NO_MOVE_PROGRESS) {
            LOG.error("Failed to move some blocks after "
                + 1 + " retries. Exiting...");
          } else if (r == ExitStatus.NO_MOVE_BLOCK) {
            LOG.error("Some blocks can't be moved. Exiting...");
          } else {
            LOG.error("Mover failed. Exiting with status " + r + "... ");
          }
          return r;
        }
        Thread.sleep(sleeptime);
        dispatcher.reset(conf);
      }
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
}
