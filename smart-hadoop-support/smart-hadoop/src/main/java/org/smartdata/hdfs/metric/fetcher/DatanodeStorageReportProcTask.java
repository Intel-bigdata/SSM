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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.action.move.Source;
import org.smartdata.hdfs.action.move.StorageGroup;
import org.smartdata.hdfs.action.move.StorageMap;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.DataNodeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DatanodeStorageReportProcTask implements Runnable {
  private static final int maxConcurrentMovesPerNode = 5;
  private DFSClient client;
  private StorageMap storages;
  private NetworkTopology networkTopology;
  private Configuration conf;
  private MetaStore metaStore;
  private volatile boolean isFinished = false;
  public static final Logger LOG =
      LoggerFactory.getLogger(DatanodeStorageReportProcTask.class);

  public DatanodeStorageReportProcTask(DFSClient client, Configuration conf, MetaStore metaStore) throws IOException {
    this.client = client;
    this.storages = new StorageMap();
    this.conf = conf;
    this.metaStore = metaStore;
  }

  public DatanodeStorageReportProcTask(DFSClient client, Configuration conf) throws IOException {
    this.client = client;
    this.storages = new StorageMap();
    this.conf = conf;
  }

  public StorageMap getStorages() {
    return storages;
  }

  public NetworkTopology getNetworkTopology() {
    return networkTopology;
  }

  public void reset() {
    storages = new StorageMap();
    networkTopology = NetworkTopology.getInstance(conf);
  }

  @Override
  public void run() {
    try {
      reset();
      final List<DatanodeStorageReport> reports = getDNStorageReports();
      for(DatanodeStorageReport r : reports) {
        // TODO: store data abstracted from reports to MetaStore
        final DDatanode dn = new DDatanode(r.getDatanodeInfo(), maxConcurrentMovesPerNode);
        for(String t : CompatibilityHelperLoader.getHelper().getMovableTypes()) {
          final Source source = dn.addSource(t);
          final long maxRemaining = getMaxRemaining(r, t);
          final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t) : null;
          storages.add(source, target);
        }
      }
      metaStore.deleteAllDataNodeInfo();
      for(DatanodeStorageReport r : reports){
        metaStore.insertDataNodeInfo(transform(r.getDatanodeInfo()));
      }
      isFinished = true;
    } catch (IOException e) {
      LOG.error("Process datanode report error", e);
    } catch (MetaStoreException e) {
      e.printStackTrace();
    }
  }

  private static long getMaxRemaining(DatanodeStorageReport report, String type) {
    long max = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (CompatibilityHelperLoader.getHelper().getStorageType(r).equals(type)) {
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
  public List<DatanodeStorageReport> getDNStorageReports() throws IOException {
    final DatanodeStorageReport[] reports =
        client.getDatanodeStorageReport(DatanodeReportType.LIVE);
    final List<DatanodeStorageReport> trimmed = new ArrayList<DatanodeStorageReport>();
    // create network topology and classify utilization collections:
    // over-utilized, above-average, below-average and under-utilized.
    for (DatanodeStorageReport r : DFSUtil.shuffle(reports)) {
      final DatanodeInfo datanode = r.getDatanodeInfo();
      trimmed.add(r);
    }
    return trimmed;
  }

  private DataNodeInfo transform(DatanodeInfo datanodeInfo) {
    return DataNodeInfo.newBuilder().setUuid(datanodeInfo.getDatanodeUuid()).
        setHostName(datanodeInfo.getHostName()).
        setIp(datanodeInfo.getIpAddr()).
        setPort(datanodeInfo.getIpcPort()).
        setCacheCapacity(datanodeInfo.getCacheCapacity()).
        setCacheUsed(datanodeInfo.getCacheUsed()).
        setLocation(datanodeInfo.getNetworkLocation()).build();
  }

  public boolean isFinished() {
    return this.isFinished;
  }
}
