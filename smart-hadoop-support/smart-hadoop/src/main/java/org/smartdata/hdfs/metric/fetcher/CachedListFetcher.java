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

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.CachedFileStatus;
import org.smartdata.model.StorageCapacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CachedListFetcher {

  private static final Long DEFAULT_INTERVAL = 5 * 1000L;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Long fetchInterval;
  private FetchTask fetchTask;
  private ScheduledFuture scheduledFuture;
  private MetaStore metaStore;

  public static final Logger LOG =
      LoggerFactory.getLogger(CachedListFetcher.class);

  public CachedListFetcher(
      Long fetchInterval,
      DFSClient dfsClient, MetaStore metaStore,
      ScheduledExecutorService service) {
    this.fetchInterval = fetchInterval;
    this.metaStore = metaStore;
    this.fetchTask = new FetchTask(dfsClient, metaStore);
    this.scheduledExecutorService = service;
  }

  public CachedListFetcher(
      Long fetchInterval,
      DFSClient dfsClient, MetaStore metaStore) {
    this(fetchInterval, dfsClient, metaStore,
        Executors.newSingleThreadScheduledExecutor());
  }

  public CachedListFetcher(
      DFSClient dfsClient, MetaStore metaStore) {
    this(DEFAULT_INTERVAL, dfsClient, metaStore,
        Executors.newSingleThreadScheduledExecutor());
  }

  public CachedListFetcher(
      DFSClient dfsClient, MetaStore metaStore,
      ScheduledExecutorService service) {
    this(DEFAULT_INTERVAL, dfsClient, metaStore, service);
  }

  public void start() {
    Long current = System.currentTimeMillis();
    Long toWait = fetchInterval - (current % fetchInterval);
    this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
        fetchTask, toWait, fetchInterval, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    if (scheduledFuture != null) {
      this.scheduledFuture.cancel(false);
    }
  }

  public List<CachedFileStatus> getCachedList() throws MetaStoreException {
    return this.metaStore.getCachedFileStatus();
  }

  private static class FetchTask extends Thread {
    private DFSClient dfsClient;
    private MetaStore metaStore;
    private Set<Long> fileSet;
    private boolean reInit;

    public FetchTask(DFSClient dfsClient, MetaStore metaStore) {
      this.dfsClient = dfsClient;
      this.metaStore = metaStore;
      reInit = true;
    }

    private void syncFromDB() {
      fileSet = new HashSet<>();
      try {
        LOG.debug("Sync CacheObject list from DB!");
        fileSet.addAll(metaStore.getCachedFids());
        reInit = false;
      } catch (MetaStoreException e) {
        LOG.error("Read fids from DB error!", e);
        reInit = true;
      }
    }

    private void clearAll() throws MetaStoreException {
      LOG.debug("CacheObject List empty!");
      if (fileSet.size() > 0) {
        metaStore.deleteAllCachedFile();
        fileSet.clear();
      }
    }

    @Override
    public void run() {
      if (reInit) {
        syncFromDB();
      }
      Set<Long> newFileSet = new HashSet<>();
      List<CachedFileStatus> cachedFileStatuses = new ArrayList<>();
      try {
        CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
        filterBuilder.setPool("SSMPool");
        CacheDirectiveInfo filter = filterBuilder.build();
        RemoteIterator<CacheDirectiveEntry> cacheDirectives =
            dfsClient.listCacheDirectives(filter);
        // Add new cache files to DB
        //get the size of SSM cache pool
        RemoteIterator<CachePoolEntry> cachePoolList = dfsClient.listCachePools();
        long cacheMaxSize = 0;
        while (cachePoolList.hasNext()) {
          CachePoolEntry cachePoolEntry = cachePoolList.next();
          if (cachePoolEntry.getInfo().getPoolName().equals("SSMPool")) {
            cacheMaxSize = cachePoolEntry.getInfo().getLimit();
          }
        }
        long cacheUsage = 0;
        if (!cacheDirectives.hasNext()) {
          metaStore.insertUpdateStoragesTable(
              new StorageCapacity("cache", cacheMaxSize, cacheMaxSize - cacheUsage));
          clearAll();
          return;
        }
        List<String> paths = new ArrayList<>();

        while (cacheDirectives.hasNext()) {
          CacheDirectiveEntry cacheDirectiveEntry = cacheDirectives.next();
          CacheDirectiveInfo currentInfo = cacheDirectiveEntry.getInfo();
          paths.add(currentInfo.getPath().toString());
          cacheUsage = cacheUsage + cacheDirectiveEntry.getStats().getBytesCached();
          LOG.debug("File in HDFS cache: " + currentInfo.getPath().toString());
        }


        //add cache information into metastore
        metaStore.insertUpdateStoragesTable(
            new StorageCapacity("cache", cacheMaxSize, cacheMaxSize - cacheUsage));

        // Delete all records to avoid conflict
        // metaStore.deleteAllCachedFile();
        // Insert new records into DB
        Map<String, Long> pathFid = metaStore.getFileIDs(paths);
        if (pathFid == null || pathFid.size() == 0) {
          clearAll();
          return;
        }
        for (String p : pathFid.keySet()) {
          long fid = pathFid.get(p);
          newFileSet.add(fid);
          if (!fileSet.contains(fid)) {
            cachedFileStatuses.add(new CachedFileStatus(fid,
                p, Time.now(), Time.now(), 0));
          }
        }
        if (cachedFileStatuses.size() != 0) {
          metaStore.insertCachedFiles(cachedFileStatuses);
        }
        // Remove uncached files from DB
        for (Long fid : fileSet) {
          if (!newFileSet.contains(fid)) {
            metaStore.deleteCachedFile(fid);
          }
        }
      } catch (MetaStoreException e) {
        LOG.error("Sync cached file list SQL error!", e);
        reInit = true;
      } catch (IOException e) {
        LOG.error("Sync cached file list HDFS error!", e);
        reInit = true;
      }
      fileSet = newFileSet;
    }
  }
}
