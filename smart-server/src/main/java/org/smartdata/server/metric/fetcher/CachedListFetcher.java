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
package org.smartdata.server.metric.fetcher;

import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.server.metastore.DBAdapter;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
  private DBAdapter dbAdapter;


  public CachedListFetcher(
      Long fetchInterval,
      DFSClient dfsClient, DBAdapter dbAdapter,
      ScheduledExecutorService service) {
    this.fetchInterval = fetchInterval;
    this.dbAdapter = dbAdapter;
    this.fetchTask = new FetchTask(dfsClient, dbAdapter);
    this.scheduledExecutorService = service;
  }

  public CachedListFetcher(
      Long fetchInterval,
      DFSClient dfsClient, DBAdapter dbAdapter) {
    this(fetchInterval, dfsClient, dbAdapter, Executors.newSingleThreadScheduledExecutor());
  }

  public CachedListFetcher(
      DFSClient dfsClient, DBAdapter dbAdapter) {
    this(DEFAULT_INTERVAL, dfsClient, dbAdapter, Executors.newSingleThreadScheduledExecutor());
  }

  public CachedListFetcher(
      DFSClient dfsClient, DBAdapter dbAdapter,
      ScheduledExecutorService service) {
    this(DEFAULT_INTERVAL, dfsClient, dbAdapter, service);
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

  public List<CachedFileStatus> getCachedList() throws SQLException {
    return this.dbAdapter.getCachedFileStatus();
  }

  private static class FetchTask extends Thread {
    private DFSClient dfsClient;
    private DBAdapter dbAdapter;
    private Set<Long> fileSet;

    public FetchTask(DFSClient dfsClient, DBAdapter dbAdapter) {
      this.dfsClient = dfsClient;
      this.dbAdapter = dbAdapter;
      this.fileSet = new HashSet<>();
    }

    @Override
    public void run() {
      try {
        CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
        filterBuilder.setPool("SSMPool");
        CacheDirectiveInfo filter = filterBuilder.build();
        RemoteIterator<CacheDirectiveEntry> cacheDirectives = dfsClient.listCacheDirectives(filter);
        Set<Long> newFileSet = new HashSet<>();
        List<CachedFileStatus> cachedFileStatuses = new ArrayList<>();
        // Add new cache files to DB
        while (cacheDirectives.hasNext()) {
          CacheDirectiveInfo currentInfo = cacheDirectives.next().getInfo();
          Long fid = currentInfo.getId();
          newFileSet.add(fid);
          if (!fileSet.contains(fid)) {
            cachedFileStatuses.add(new CachedFileStatus(currentInfo.getId(),
                currentInfo.getPath().toString(), Time.now(), Time.now(), 0));
          }
        }
        if (cachedFileStatuses.size() != 0) {
          // Delete all records to avoid conflict
          dbAdapter.deleteAllCachedFile();
          // Insert new records into DB
          dbAdapter.insertCachedFiles(cachedFileStatuses);
        }
        // Remove uncached files from DB
        // for (Long fid : fileSet) {
        //   if (!newFileSet.contains(fid)) {
        //     dbAdapter.deleteCachedFile(fid);
        //   }
        // }
        fileSet = newFileSet;
      } catch (IOException e) {
        e.printStackTrace();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
