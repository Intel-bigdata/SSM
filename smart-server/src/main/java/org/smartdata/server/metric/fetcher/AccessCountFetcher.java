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

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.namenode.metrics.FileAccessMetrics;
import org.apache.hadoop.util.Time;
import org.smartdata.server.metastore.sql.tables.AccessCountTableManager;
import org.smartdata.server.utils.FileAccessEvent;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AccessCountFetcher {
  private static final Long DEFAULT_INTERVAL = 5 * 1000L;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Long fetchInterval;
  private ScheduledFuture scheduledFuture;
  private FetchTask fetchTask;

  public AccessCountFetcher(
      DFSClient client, AccessCountTableManager manager, ScheduledExecutorService service) {
    this(DEFAULT_INTERVAL, client, manager, service);
  }

  public AccessCountFetcher(Long fetchInterval, DFSClient client,
      AccessCountTableManager manager) {
    this(fetchInterval, client, manager, Executors.newSingleThreadScheduledExecutor());
  }

  public AccessCountFetcher(Long fetchInterval, DFSClient client,
      AccessCountTableManager manager, ScheduledExecutorService service) {
    this.fetchInterval = fetchInterval;
    this.fetchTask = new FetchTask(client, manager);
    this.scheduledExecutorService = service;
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

  private static class FetchTask implements Runnable {
    private final DFSClient client;
    private final AccessCountTableManager manager;
    private FileAccessMetrics.Reader reader;
    private long now;

    public FetchTask(DFSClient client, AccessCountTableManager manager) {
      this.client = client;
      this.manager = manager;
      try {
        this.reader = FileAccessMetrics.Reader.create();
      } catch (IOException | URISyntaxException e) {
        e.printStackTrace();
      }
      now = Time.now();
    }

    @Override
    public void run() {
      try {
        if (reader.exists(now)) {
          reader.seekTo(now, false);

          List<FileAccessEvent> events = new ArrayList<>();
          while (reader.hasNext()) {
            FileAccessMetrics.Info info = reader.next();
            events.add(new FileAccessEvent(info.getPath(), info.getTimestamp()));
            System.out.println(info.getPath() + " " + info.getTimestamp());
            now = info.getTimestamp();
          }
          if(events.size() > 0) {
            this.manager.onAccessEventsArrived(events);
          }
        }
      } catch (IOException | URISyntaxException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
}
