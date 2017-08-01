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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Fetch and maintain data nodes related info.
 */
public class DataNodeInfoFetcher {
  private static final long DN_STORAGE_REPORT_UPDATE_INTERVAL = 10 * 1000;
  private final DFSClient client;
  private final MetaStore metaStore;
  private final ScheduledExecutorService scheduledExecutorService;
  private ScheduledFuture dnStorageReportProcTaskFuture;
  public static final Logger LOG =
      LoggerFactory.getLogger(DataNodeInfoFetcher.class);

  public DataNodeInfoFetcher(DFSClient client, MetaStore metaStore,
      ScheduledExecutorService service) {
    this.client = client;
    this.metaStore = metaStore;
    this.scheduledExecutorService = service;
  }

  public void start() throws IOException {
    LOG.info("Starting DataNodeInfoFetcher service ...");

    DatanodeStorageReportProcTask procTask = new DatanodeStorageReportProcTask(client);
    dnStorageReportProcTaskFuture = scheduledExecutorService.scheduleAtFixedRate(
        procTask, 0, DN_STORAGE_REPORT_UPDATE_INTERVAL, TimeUnit.MILLISECONDS);

    LOG.info("DataNodeInfoFetcher service started.");
  }

  public void stop() {
    if (dnStorageReportProcTaskFuture != null) {
      dnStorageReportProcTaskFuture.cancel(false);
    }
  }

  private static class DatanodeStorageReportProcTask implements Runnable {
    private DFSClient client;
    private NetworkTopology cluster;

    public DatanodeStorageReportProcTask(DFSClient client) throws IOException {
      this.client = client;
    }

    @Override
    public void run() {
      try {
        final List<DatanodeStorageReport> reports = init();
//        for(DatanodeStorageReport r : reports) {
//          // TODO: store data abstracted from reports to MetaStore
//          final DDatanode dn = new DDatanode(r.getDatanodeInfo());
//          for(StorageType t : StorageType.getMovableTypes()) {
//            final Source source = dn.addSource(t, Long.MAX_VALUE, dispatcher);
//            final long maxRemaining = getMaxRemaining(r, t);
//            final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t,
//                maxRemaining) : null;
//            storages.add(source, target);
//          }
//        }
      } catch (IOException e) {
        LOG.error("Process datanode report error", e);
      }
    }

    private static long getMaxRemaining(DatanodeStorageReport report, StorageType t) {
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

    /**
     * Get live datanode storage reports and then build the network topology.
     * @return
     * @throws IOException
     */
    public List<DatanodeStorageReport> init() throws IOException {
      final DatanodeStorageReport[] reports =
          client.getDatanodeStorageReport(DatanodeReportType.LIVE);
      final List<DatanodeStorageReport> trimmed = new ArrayList<DatanodeStorageReport>();
      // create network topology and classify utilization collections:
      // over-utilized, above-average, below-average and under-utilized.
      for (DatanodeStorageReport r : DFSUtil.shuffle(reports)) {
        final DatanodeInfo datanode = r.getDatanodeInfo();
        trimmed.add(r);
        cluster.add(datanode);
      }
      return trimmed;
    }
  }
}
