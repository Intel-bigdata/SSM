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
package org.smartdata.alluxio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.alluxio.metric.fetcher.AlluxioNamespaceFetcher;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.StatesUpdaterService;

import alluxio.client.file.FileSystem;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
/**
 * Polls metrics and events from Alluxio Server
 */
public class AlluxioStatesUpdaterService extends StatesUpdaterService {

  private FileSystem alluxioFs;
  private ScheduledExecutorService executorService;
  private AlluxioNamespaceFetcher namespaceFetcher;

  public static final Logger LOG =
      LoggerFactory.getLogger(AlluxioStatesUpdaterService.class);

  public AlluxioStatesUpdaterService(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
  }

  /**
   * Load configure/data to initialize.
   *
   * @return true if initialized successfully
   */
  @Override
  public void init() throws IOException {
    LOG.info("Initializing ...");
    SmartContext context = getContext();
    this.alluxioFs = FileSystem.Factory.get();
    this.executorService = Executors.newScheduledThreadPool(4);
    this.namespaceFetcher = new AlluxioNamespaceFetcher(alluxioFs, metaStore,
        AlluxioNamespaceFetcher.DEFAULT_INTERVAL, executorService);
    LOG.info("Initialized.");
  }

  /**
   * Start daemon threads in StatesManager for function.
   */
  @Override
  public void start() throws IOException {
    LOG.info("Starting ...");
    this.namespaceFetcher.startFetch();
    LOG.info("Started. ");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");
    if (this.namespaceFetcher != null) {
      this.namespaceFetcher.stop();
    }
    LOG.info("Stopped.");
  }

}
