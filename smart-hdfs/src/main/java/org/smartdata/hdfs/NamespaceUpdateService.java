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
package org.smartdata.hdfs;

import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.hdfs.metric.fetcher.CachedListFetcher;
import org.smartdata.hdfs.metric.fetcher.InotifyEventFetcher;
import org.smartdata.server.engine.MetaStore;
import org.smartdata.server.engine.ServerContext;
import org.smartdata.server.utils.HadoopUtils;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Polls metrics and events from NameNode
 */
public class NamespaceUpdateService extends AbstractService {
  private ServerContext serverContext;

  private DFSClient client;
  private ScheduledExecutorService executorService;
  private InotifyEventFetcher inotifyEventFetcher;
  private CachedListFetcher cachedListFetcher;

  public static final Logger LOG =
      LoggerFactory.getLogger(NamespaceUpdateService.class);

  public NamespaceUpdateService(ServerContext context) {
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
    this.cleanFileTableContents(serverContext.getMetaStore());
    URI nnUri = HadoopUtils.getNameNodeUri(serverContext.getConf());
    this.client = new DFSClient(nnUri, serverContext.getConf());
    this.executorService = Executors.newScheduledThreadPool(4);
    this.cachedListFetcher = new CachedListFetcher(client, serverContext.getMetaStore());
    this.inotifyEventFetcher = new InotifyEventFetcher(client,
        serverContext.getMetaStore(), executorService);
    LOG.info("Initialized.");
  }

  /**
   * Start daemon threads in StatesManager for function.
   */
  @Override
  public void start() throws IOException {
    LOG.info("Starting ...");
    this.inotifyEventFetcher.start();
    this.cachedListFetcher.start();
    LOG.info("Started. ");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");
    if (inotifyEventFetcher != null) {
      this.inotifyEventFetcher.stop();
    }

    if (this.cachedListFetcher != null) {
      this.cachedListFetcher.stop();
    }
    LOG.info("Stopped.");
  }

  private void cleanFileTableContents(MetaStore adapter) throws IOException {
    try {
      adapter.execute("DELETE FROM files");
    } catch (SQLException e) {
      throw new IOException("Error while 'DELETE FROM files'", e);
    }
  }
}
