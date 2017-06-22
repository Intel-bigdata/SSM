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
package org.smartdata.server.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventSource;
import org.smartdata.metrics.impl.MetricsFactory;
import org.smartdata.server.engine.data.AccessEventFetcher;
import org.smartdata.server.engine.metastore.FileAccessInfo;
import org.smartdata.server.engine.metastore.tables.AccessCountTable;
import org.smartdata.server.engine.metastore.tables.AccessCountTableManager;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Polls metrics and events from NameNode
 */
public class StatesManager extends AbstractService {
  private ServerContext serverContext;

  private ScheduledExecutorService executorService;
  private AccessCountTableManager accessCountTableManager;
  private AccessEventFetcher accessEventFetcher;
  private FileAccessEventSource fileAccessEventSource;
  private AbstractService nameSpaceService;

  public static final Logger LOG = LoggerFactory.getLogger(StatesManager.class);

  public StatesManager(ServerContext context) {
    super(context);
    this.serverContext = context;
  }

  /**
   * Load configure/data to initialize.
   *
   * @return true if initialized successfully
   */
  @Override
  public void init() throws IOException {
    LOG.info("Initializing ...");
    this.executorService = Executors.newScheduledThreadPool(4);
    this.accessCountTableManager = new AccessCountTableManager(
        serverContext.getMetaStore(), executorService);
    this.fileAccessEventSource = MetricsFactory.createAccessEventSource(serverContext.getConf());
    this.accessEventFetcher =
        new AccessEventFetcher(
            serverContext.getConf(), accessCountTableManager,
            executorService, fileAccessEventSource.getCollector());
    nameSpaceService =
        StatesUpdaterServiceFactory.createStatesUpdaterService(serverContext.getConf());
    nameSpaceService.setContext(serverContext);
    LOG.info("Initialized.");
  }

  /**
   * Start daemon threads in StatesManager for function.
   */
  @Override
  public void start() throws IOException {
    LOG.info("Starting ...");
    this.accessEventFetcher.start();
    LOG.info("Started. ");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");

    if (accessEventFetcher != null) {
      this.accessEventFetcher.stop();
    }
    if (this.fileAccessEventSource != null) {
      this.fileAccessEventSource.close();
    }
    LOG.info("Stopped.");
  }

  public List<CachedFileStatus> getCachedList() throws SQLException {
    return serverContext.getMetaStore().getCachedFileStatus();
  }

  public List<AccessCountTable> getTablesInLast(long timeInMills) throws SQLException {
    return this.accessCountTableManager.getTables(timeInMills);
  }

  public void reportFileAccessEvent(FileAccessEvent event) throws IOException {
    event.setTimeStamp(System.currentTimeMillis());
    this.fileAccessEventSource.insertEventFromSmartClient(event);
  }

  public List<FileAccessInfo> getHotFiles(List<AccessCountTable> tables,
      int topNum) throws IOException {
    try {
      return serverContext.getMetaStore().getHotFiles(tables, topNum);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  public List<CachedFileStatus> getCachedFileStatus() throws IOException {
    try {
      return serverContext.getMetaStore().getCachedFileStatus();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
