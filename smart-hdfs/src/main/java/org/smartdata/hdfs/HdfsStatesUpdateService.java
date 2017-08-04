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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.metric.fetcher.CachedListFetcher;
import org.smartdata.hdfs.metric.fetcher.DataNodeInfoFetcher;
import org.smartdata.hdfs.metric.fetcher.InotifyEventFetcher;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metastore.StatesUpdateService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Polls metrics and events from NameNode
 */
public class HdfsStatesUpdateService extends StatesUpdateService {
  private static final Path MOVER_ID_PATH = new Path("/system/mover.id");
  private DFSClient client;
  private ScheduledExecutorService executorService;
  private InotifyEventFetcher inotifyEventFetcher;
  private CachedListFetcher cachedListFetcher;
  private DataNodeInfoFetcher dataNodeInfoFetcher;
  private FSDataOutputStream moverIdOutputStream;

  public static final Logger LOG =
      LoggerFactory.getLogger(HdfsStatesUpdateService.class);

  public HdfsStatesUpdateService(SmartContext context, MetaStore metaStore) {
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
    URI nnUri = HadoopUtil.getNameNodeUri(context.getConf());
    this.client = new DFSClient(nnUri, context.getConf());
    moverIdOutputStream = checkAndMarkRunning(nnUri, context.getConf());
    this.cleanFileTableContents(metaStore);
    this.executorService = Executors.newScheduledThreadPool(4);
    this.cachedListFetcher = new CachedListFetcher(client, metaStore);
    this.inotifyEventFetcher = new InotifyEventFetcher(client,
        metaStore, executorService);
    this.dataNodeInfoFetcher = new DataNodeInfoFetcher(
        client, metaStore, executorService);
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
    this.dataNodeInfoFetcher.start();
    LOG.info("Started. ");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");
    if (moverIdOutputStream != null) {
      try {
        moverIdOutputStream.close();
      } catch (IOException e) {
        LOG.debug("Close 'mover' ID output stream error", e);
        // ignore this
      }
    }

    if (inotifyEventFetcher != null) {
      this.inotifyEventFetcher.stop();
    }

    if (this.cachedListFetcher != null) {
      this.cachedListFetcher.stop();
    }

    if (dataNodeInfoFetcher != null) {
      dataNodeInfoFetcher.stop();
    }
    LOG.info("Stopped.");
  }

  private void cleanFileTableContents(MetaStore adapter) throws IOException {
    try {
      adapter.execute("DELETE FROM files");
    } catch (MetaStoreException e) {
      throw new IOException("Error while 'DELETE FROM files'", e);
    }
  }

  private FSDataOutputStream checkAndMarkRunning(URI namenodeURI, Configuration conf)
      throws IOException {
    try {
      DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(namenodeURI, conf);
      if (fs.exists(MOVER_ID_PATH)) {
        // try appending to it so that it will fail fast if another instance is
        // running.
        IOUtils.closeStream(fs.append(MOVER_ID_PATH));
        fs.delete(MOVER_ID_PATH, true);
      }
      FSDataOutputStream fsout = fs.create(MOVER_ID_PATH, false);
      fs.deleteOnExit(MOVER_ID_PATH);
      fsout.writeBytes(InetAddress.getLocalHost().getHostName());
      fsout.hflush();
      return fsout;
    } catch (Exception e) {
      LOG.error("Unable to lock 'mover', please stop 'mover' first.");
      throw e;
    }
  }
}
