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
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.StorageCapacity;
import org.smartdata.utils.StringUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Storage information sampling.
 */
public class StorageInfoSampler {
  private MetaStore metaStore;
  private Configuration conf;
  private Map<Long, Integer> samplingIntervals;
  private ScheduledExecutorService service;
  private static final Logger LOG =
      LoggerFactory.getLogger(StorageInfoSampler.class);

  public StorageInfoSampler(MetaStore metaStore, Configuration conf) throws IOException {
    this.metaStore = metaStore;
    this.conf = conf;
    samplingIntervals = getSamplingConfiguration();
    this.service = Executors.newScheduledThreadPool(1);
  }

  public void start() throws IOException {
    LOG.info("Starting storage sampling service ...");
    long curr = System.currentTimeMillis();
    for (Long intval : samplingIntervals.keySet()) {
      long initDelay = intval - (curr % intval);
      service.scheduleAtFixedRate(new InfoSamplingTask(intval, samplingIntervals.get(intval)),
          initDelay, intval, TimeUnit.MILLISECONDS);
    }
    LOG.info("Storage sampling service started.");
  }

  public void stop() {
    if (service != null) {
      service.shutdown();
    }
  }

  private Map<Long, Integer> getSamplingConfiguration() throws IOException {
    String samplingStr = conf.get(SmartConfKeys.SMART_STORAGE_INFO_SAMPLING_INTERVALS_KEY,
        SmartConfKeys.SMART_STORAGE_INFO_SAMPLING_INTERVALS_DEFAULT);
    String[] items = samplingStr.split(";");
    Map<Long, Integer> ret = new HashMap<>();
    for (String s : items) {
      if (!s.equals("")) {
        String[] samples = s.split(",");
        Long interval = StringUtil.pharseTimeString(samples[0]);
        Integer maxNum;
        if (samples.length == 2) {
          maxNum = Integer.valueOf(samples[1]);
        } else if (samples.length == 1) {
          maxNum = Integer.MAX_VALUE;
        } else {
          throw new IOException("Invalid value format for configure option '"
              + SmartConfKeys.SMART_STORAGE_INFO_SAMPLING_INTERVALS_KEY + "' = '"
              + samplingStr + "' on part '" + s + "'");
        }
        ret.put(interval, maxNum);
      }
    }
    return ret;
  }

  private class InfoSamplingTask implements Runnable {
    private long interval;
    private long maxItems;
    private boolean clean = false;

    public InfoSamplingTask(long interval, int maxItems) {
      this.interval = interval;
      this.maxItems = maxItems;
      clean = System.currentTimeMillis() - (maxItems + 1L) * interval > 0;
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
        if (clean) {
          for (String t : capacities.keySet()) {
            metaStore.deleteStorageHistoryOldRecords(t, interval,
                curr - maxItems * interval - interval / 2);
          }
        }
      } catch (Throwable t) {
        LOG.error("Storage info sampling task error: ", t);
      }
    }
  }
}