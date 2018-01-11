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
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.DataNodeInfo;
import org.smartdata.model.DataNodeStorageInfo;
import org.smartdata.model.StorageCapacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Fetch and maintain data nodes related info.
 */
public class DataNodeInfoFetcher {
  private long updateInterval;
  private final DFSClient client;
  private final MetaStore metaStore;
  private final ScheduledExecutorService scheduledExecutorService;
  private ScheduledFuture dnStorageReportProcTaskFuture;
  private Configuration conf;
  private DataNodeInfoFetchTask procTask;
  public static final Logger LOG =
      LoggerFactory.getLogger(DataNodeInfoFetcher.class);

  public DataNodeInfoFetcher(DFSClient client, MetaStore metaStore,
                             ScheduledExecutorService service, Configuration conf) {
    this.client = client;
    this.metaStore = metaStore;
    this.scheduledExecutorService = service;
    this.conf = conf;
    updateInterval = conf.getInt(SmartConfKeys.SMART_STORAGE_INFO_UPDATE_INTERVAL_KEY,
        SmartConfKeys.SMART_STORAGE_INFO_UPDATE_INTERVAL_DEFAULT) * 1000;
  }

  public void start() throws IOException {
    LOG.info("Starting DataNodeInfoFetcher service ...");

    procTask = new DataNodeInfoFetchTask(client, conf, metaStore);
    dnStorageReportProcTaskFuture = scheduledExecutorService.scheduleAtFixedRate(
        procTask, 0, updateInterval, TimeUnit.MILLISECONDS);

    LOG.info("DataNodeInfoFetcher service started.");
  }

  public boolean isFetchFinished() {
    return this.procTask.isFinished();
  }

  public void stop() {
    if (dnStorageReportProcTaskFuture != null) {
      dnStorageReportProcTaskFuture.cancel(false);
    }
  }

  private class DataNodeInfoFetchTask implements Runnable {
    private DFSClient client;
    private Configuration conf;
    private MetaStore metaStore;
    private volatile boolean isFinished = false;
    private Map<String, StorageCapacity> storages;
    public final Logger LOG =
        LoggerFactory.getLogger(DataNodeInfoFetchTask.class);

    public DataNodeInfoFetchTask(DFSClient client, Configuration conf, MetaStore metaStore)
        throws IOException {
      this.client = client;
      this.conf = conf;
      this.metaStore = metaStore;

      try {
        storages = metaStore.getStorageCapacity();
      } catch (MetaStoreException e) {
        throw new IOException("Can not get storage info");
      }
    }

    @Override
    public void run() {
      StorageCapacity sc;
      Map<String, StorageCapacity> storagesNow = new HashMap<>();
      try {
        final List<DatanodeStorageReport> reports = getDNStorageReports();
        metaStore.deleteAllDataNodeInfo();
        for (DatanodeStorageReport r : reports) {
          metaStore.insertDataNodeInfo(transform(r.getDatanodeInfo()));
          List<DataNodeStorageInfo> infos = new ArrayList<>();
          //insert record in DataNodeStorageInfoTable
          for (int i = 0; i < r.getStorageReports().length; i++) {
            StorageReport storageReport = r.getStorageReports()[i];
            long sid = CompatibilityHelperLoader.getHelper().getSidInDatanodeStorageReport(
                storageReport.getStorage());
            String uuid = r.getDatanodeInfo().getDatanodeUuid();
            long state = storageReport.getStorage().getState().ordinal();
            String storageId = storageReport.getStorage().getStorageID();
            long fail = 1;
            if (!storageReport.isFailed()) {
              fail = 0;
            }
            long capacity = storageReport.getCapacity();
            long dfsUsed = storageReport.getDfsUsed();
            long remaining = storageReport.getRemaining();
            long blockPoolUsed = storageReport.getBlockPoolUsed();
            infos.add(new DataNodeStorageInfo(uuid, sid, state,
                storageId, fail, capacity, dfsUsed, remaining, blockPoolUsed));

            String sn = storageReport.getStorage().getStorageType().name();
            if (!storagesNow.containsKey(sn)) {
              sc = new StorageCapacity(sn, capacity, remaining);
              storagesNow.put(sn, sc);
            } else {
              sc = storagesNow.get(sn);
              sc.addCapacity(capacity);
              sc.addFree(remaining);
            }
          }
          metaStore.deleteDataNodeStorageInfo(r.getDatanodeInfo().getDatanodeUuid());
          metaStore.insertDataNodeStorageInfos(infos);
        }
        updateStorages(storagesNow);
        storages = storagesNow;
        isFinished = true;
      } catch (IOException e) {
        LOG.error("Process datanode report error", e);
      } catch (MetaStoreException e) {
        LOG.error("Process datanode report error", e);
      }
    }

    private void updateStorages(Map<String, StorageCapacity> storagesNow)
        throws MetaStoreException {
      String k;
      StorageCapacity v;
      List<StorageCapacity> sc = new ArrayList<>();
      for (Entry<String, StorageCapacity> kv : storages.entrySet()) {
        k = kv.getKey();
        if (storagesNow.containsKey(k)) {
          v = storagesNow.get(k);
          if (!kv.getValue().equals(v)) {
            sc.add(v);
          }
        } else {
          metaStore.deleteStorage(kv.getKey());
        }
      }

      for (Entry<String, StorageCapacity> kv : storagesNow.entrySet()) {
        if (!storages.containsKey(kv.getKey())) {
          sc.add(kv.getValue());
        }
      }
      metaStore.insertUpdateStoragesTable(sc);
    }

    /**
     * Get live datanode storage reports and then build the network topology.
     * @return
     * @throws IOException
     */
    public List<DatanodeStorageReport> getDNStorageReports() throws IOException {
      final DatanodeStorageReport[] reports =
          client.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.LIVE);
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
          setRpcAddress(datanodeInfo.getIpAddr() + ":" + 
              Integer.toString(datanodeInfo.getIpcPort())).
          setCacheCapacity(datanodeInfo.getCacheCapacity()).
          setCacheUsed(datanodeInfo.getCacheUsed()).
          setLocation(datanodeInfo.getNetworkLocation()).build();
    }

    public boolean isFinished() {
      return this.isFinished;
    }
  }
}
