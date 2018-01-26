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
import org.smartdata.SmartConstants;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.metric.fetcher.CachedListFetcher;
import org.smartdata.hdfs.metric.fetcher.DataNodeInfoFetcher;
import org.smartdata.hdfs.metric.fetcher.InotifyEventFetcher;
import org.smartdata.hdfs.metric.fetcher.StorageInfoSampler;
import org.smartdata.hdfs.ruleplugin.CheckSsdRulePlugin;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.StatesUpdateService;
import org.smartdata.model.rule.RulePluginManager;
import org.smartdata.utils.SsmHostUtils;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Polls metrics and events from NameNode
 */
public class HdfsStatesUpdateService extends StatesUpdateService {
  private static final String MOVER_ID_PATH = "/system/mover.id";
  private volatile boolean inSafeMode;
  private DFSClient client;
  private ScheduledExecutorService executorService;
  private InotifyEventFetcher inotifyEventFetcher;
  private CachedListFetcher cachedListFetcher;
  private DataNodeInfoFetcher dataNodeInfoFetcher;
  private FSDataOutputStream moverIdOutputStream;
  private FSDataOutputStream ssmIdOutputStream;
  private StorageInfoSampler storageInfoSampler;

  public static final Logger LOG =
      LoggerFactory.getLogger(HdfsStatesUpdateService.class);

  public HdfsStatesUpdateService(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    this.inSafeMode = true;
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
    final Configuration conf = context.getConf();
    String hadoopConfPath = getContext().getConf()
        .get(SmartConfKeys.SMART_HADOOP_CONF_DIR_KEY);
    try {
      HadoopUtil.loadHadoopConf(context.getConf(), hadoopConfPath);
    } catch (IOException e) {
      throw new IOException("Fail to load Hadoop configuration for : " + e.getMessage());
    }
    final URI nnUri = HadoopUtil.getNameNodeUri(context.getConf());
    LOG.debug("Final Namenode URL:" + nnUri.toString());
    client = HadoopUtil.getDFSClient(nnUri, conf);
    checkAndCreateIdFiles(nnUri, context.getConf());
    this.executorService = Executors.newScheduledThreadPool(4);
    this.cachedListFetcher = new CachedListFetcher(client, metaStore);
    this.inotifyEventFetcher = new InotifyEventFetcher(client,
        metaStore, executorService, new FetchFinishedCallBack(), context.getConf());
    this.dataNodeInfoFetcher = new DataNodeInfoFetcher(
        client, metaStore, executorService, context.getConf());
    this.storageInfoSampler = new StorageInfoSampler(metaStore, conf);
    LOG.info("Initialized.");
  }

  private class FetchFinishedCallBack implements Callable<Object> {
    @Override
    public Object call() throws Exception {
      inSafeMode = false;
      return null;
    }
  }

  @Override
  public boolean inSafeMode() {
    return inSafeMode;
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
    this.storageInfoSampler.start();
    RulePluginManager.addPlugin(new CheckSsdRulePlugin(metaStore));
    LOG.info("Started. ");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");
    if (moverIdOutputStream != null) {
      try {
        moverIdOutputStream.close();
        moverIdOutputStream = null;
      } catch (IOException e) {
        LOG.debug("Close 'mover' ID output stream error", e);
        // ignore this
      }
    }

    if (ssmIdOutputStream != null) {
      try {
        ssmIdOutputStream.close();
        ssmIdOutputStream = null;
      } catch (IOException e) {
        LOG.debug("Close SSM ID output stream error", e);
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

    if (storageInfoSampler != null) {
      storageInfoSampler.stop();
    }
    LOG.info("Stopped.");
  }

  private void checkAndCreateIdFiles(URI namenodeURI, Configuration conf) throws IOException {
    try {
      moverIdOutputStream = checkAndMarkRunning(namenodeURI, conf, MOVER_ID_PATH);
      LOG.info("Mover ID file " + MOVER_ID_PATH + " created successfully.");
    } catch (IOException e) {
      LOG.error("Unable to create " + MOVER_ID_PATH + " in HDFS. "
          + "Please check the permission or if it is being written by another instance.");
      throw e;
    }

    try {
      ssmIdOutputStream = checkAndMarkRunning(namenodeURI, conf,
          SmartConstants.SMART_SERVER_ID_FILE);
      LOG.info("Smart server ID file " + SmartConstants.SMART_SERVER_ID_FILE
          + " created successfully.");
    } catch (IOException e) {
      LOG.error("Unable to create SSM ID file: " + SmartConstants.SMART_SERVER_ID_FILE
          + " in HDFS. Please check the permission or if it is being written by "
          + "another instance.");
      try {
        moverIdOutputStream.close();
        moverIdOutputStream = null;
      } catch (IOException ie) {
        // ignore this one
      }
      throw e;
    }
  }

  private FSDataOutputStream checkAndMarkRunning(URI namenodeURI, Configuration conf, String filePath)
      throws IOException {
    Path path = new Path(filePath);
    DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(namenodeURI, conf);
    if (fs.exists(path)) {
      // try appending to it so that it will fail fast if another instance is
      // running.
      IOUtils.closeStream(fs.append(path));
      fs.delete(path, true);
    }
    FSDataOutputStream fsout = fs.create(path, false);
    try {
      fs.deleteOnExit(path);
      fsout.writeBytes(SsmHostUtils.getHostNameOrIp());
      fsout.hflush();
    } catch (IOException e) {
      fsout.close();
      throw e;
    }
    return fsout;
  }
}
