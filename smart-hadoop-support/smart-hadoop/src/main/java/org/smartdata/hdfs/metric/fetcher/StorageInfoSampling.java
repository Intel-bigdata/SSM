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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.StorageCapacity;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Storage information sampling.
 */
public class StorageInfoSampling {
  private MetaStore metaStore;
  private Configuration conf;
  private ScheduledExecutorService service;
  private static final Logger LOG =
      LoggerFactory.getLogger(StorageInfoSampling.class);

  public StorageInfoSampling(MetaStore metaStore, Configuration conf) {
    this.metaStore = metaStore;
    this.conf = conf;
    this.service = Executors.newScheduledThreadPool(1);
  }

  public void start() throws IOException {
    LOG.info("Starting storage sampling service ...");

    LOG.info("Storage sampling service started.");
  }

  public void stop() {
    if (service != null) {
      service.shutdown();
    }
  }

  private class InfoSamplingTask implements Runnable {
    private long interval;
    private long maxItems;

    public InfoSamplingTask(long interval, int maxItems) {
      this.interval = interval;
      this.maxItems = maxItems;
    }

    @Override
    public void run() {
      long curr = System.currentTimeMillis();
      Map<String, StorageCapacity> capacities;
      try {
        capacities = metaStore.getStorageCapacity();
        for (StorageCapacity c : capacities.values()) {
          c.setTimeStamp(curr);
        }
        metaStore.insertStorageHistTable(
            capacities.values().toArray(new StorageCapacity[capacities.size()]), interval);
        for (String t : capacities.keySet()) {
          metaStore.deleteStorageHistoryOldRecords(t, interval,
              curr - maxItems * interval - interval / 2);
        }
      } catch (Throwable t) {
        LOG.error("Storage info sampling task error: ", t);
      }
    }
  }
}