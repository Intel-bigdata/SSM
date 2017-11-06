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
package org.smartdata.server.engine.data;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.dao.AccessCountTableManager;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventCollector;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AccessEventFetcher {
  static final Logger LOG = LoggerFactory.getLogger(AccessEventFetcher.class);

  private static final Long DEFAULT_INTERVAL = 1 * 1000L;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Long fetchInterval;
  private ScheduledFuture scheduledFuture;
  private FetchTask fetchTask;

  public AccessEventFetcher(
      Configuration conf,
      AccessCountTableManager manager,
      ScheduledExecutorService service,
      FileAccessEventCollector collector) {
    this(DEFAULT_INTERVAL, conf, manager, service, collector);
  }

  public AccessEventFetcher(
      Long fetchInterval,
      Configuration conf,
      AccessCountTableManager manager,
      FileAccessEventCollector collector) {
    this(fetchInterval, conf, manager, Executors.newSingleThreadScheduledExecutor(), collector);
  }

  public AccessEventFetcher(
      Long fetchInterval,
      Configuration conf,
      AccessCountTableManager manager,
      ScheduledExecutorService service,
      FileAccessEventCollector collector) {
    this.fetchInterval = fetchInterval;
    this.fetchTask = new FetchTask(conf, manager, collector);
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
    private final Configuration conf;
    private final AccessCountTableManager manager;
    private final FileAccessEventCollector collector;

    public FetchTask(
        Configuration conf, AccessCountTableManager manager, FileAccessEventCollector collector) {
      this.conf = conf;
      this.manager = manager;
      this.collector = collector;
    }

    @Override
    public void run() {
      try {
        List<FileAccessEvent> events = this.collector.collect();
        if (events.size() > 0) {
          this.manager.onAccessEventsArrived(events);
        }
      } catch (IOException e) {
        LOG.error("IngestionTask onAccessEventsArrived error", e);
      }
    }
  }
}
