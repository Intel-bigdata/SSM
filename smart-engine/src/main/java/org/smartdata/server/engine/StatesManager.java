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
import org.smartdata.conf.Reconfigurable;
import org.smartdata.conf.ReconfigurableRegistry;
import org.smartdata.conf.ReconfigureException;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metastore.dao.AccessCountTable;
import org.smartdata.metastore.dao.AccessCountTableManager;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventSource;
import org.smartdata.metrics.impl.MetricsFactory;
import org.smartdata.model.CachedFileStatus;
import org.smartdata.model.FileAccessInfo;
import org.smartdata.model.FileInfo;
import org.smartdata.model.StorageCapacity;
import org.smartdata.model.Utilization;
import org.smartdata.server.engine.data.AccessEventFetcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Polls metrics and events from NameNode.
 */
public class StatesManager extends AbstractService implements Reconfigurable {
  private ServerContext serverContext;

  private ScheduledExecutorService executorService;
  private AccessCountTableManager accessCountTableManager;
  private AccessEventFetcher accessEventFetcher;
  private FileAccessEventSource fileAccessEventSource;
  private AbstractService statesUpdaterService;
  private volatile boolean working = false;
  private List<String> ignoreDirs = new ArrayList<String>();

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

    initStatesUpdaterService();
    if (statesUpdaterService == null) {
      ReconfigurableRegistry.registReconfigurableProperty(
          getReconfigurableProperties(), this);
    }

    Collection<String> dirs = serverContext.getConf()
        .getTrimmedStringCollection(SmartConfKeys.SMART_IGNORE_DIRS_KEY);
    for (String s : dirs) {
      if (!s.endsWith("/")) {
        s = s + "/";
        ignoreDirs.add(s);
      }
    }
    LOG.info("Initialized.");
  }

  @Override
  public boolean inSafeMode() {
    if (statesUpdaterService == null) {
      return true;
    }
    return statesUpdaterService.inSafeMode();
  }

  /**
   * Start daemon threads in StatesManager for function.
   */
  @Override
  public void start() throws IOException {
    LOG.info("Starting ...");
    accessEventFetcher.start();
    if (statesUpdaterService != null) {
      statesUpdaterService.start();
    }
    working = true;
    LOG.info("Started. ");
  }

  @Override
  public void stop() throws IOException {
    working = false;
    LOG.info("Stopping ...");

    if (accessEventFetcher != null) {
      this.accessEventFetcher.stop();
    }
    if (this.fileAccessEventSource != null) {
      this.fileAccessEventSource.close();
    }
    if (statesUpdaterService != null) {
      statesUpdaterService.stop();
    }
    LOG.info("Stopped.");
  }

  public List<CachedFileStatus> getCachedList() throws MetaStoreException {
    return serverContext.getMetaStore().getCachedFileStatus();
  }

  public List<AccessCountTable> getTablesInLast(long timeInMills) throws MetaStoreException {
    return this.accessCountTableManager.getTables(timeInMills);
  }

  public void reportFileAccessEvent(FileAccessEvent event) throws IOException {
    String path = event.getPath();
    path = path + (path.endsWith("/") ? "" : "/");
    for (String s : ignoreDirs) {
      if (path.startsWith(s)) {
        return;
      }
    }
    event.setTimeStamp(System.currentTimeMillis());
    this.fileAccessEventSource.insertEventFromSmartClient(event);
  }

  public List<FileAccessInfo> getHotFiles(List<AccessCountTable> tables,
      int topNum) throws IOException {
    try {
      return serverContext.getMetaStore().getHotFiles(tables, topNum);
    } catch (MetaStoreException e) {
      throw new IOException(e);
    }
  }

  public List<CachedFileStatus> getCachedFileStatus() throws IOException {
    try {
      return serverContext.getMetaStore().getCachedFileStatus();
    } catch (MetaStoreException e) {
      throw new IOException(e);
    }
  }

  public Utilization getStorageUtilization(String resourceName) throws IOException {
    try {
      long now = System.currentTimeMillis();
      if (!resourceName.equals("cache")) {
        long capacity =
            serverContext.getMetaStore().getStoreCapacityOfDifferentStorageType(resourceName);
        long free = serverContext.getMetaStore().getStoreFreeOfDifferentStorageType(resourceName);
        return new Utilization(now, capacity, capacity - free);
      } else {
        StorageCapacity storageCapacity = serverContext.getMetaStore().getStorageCapacity("cache");
        return new Utilization(now,
            storageCapacity.getCapacity(),
            storageCapacity.getCapacity() - storageCapacity.getFree());
      }
    } catch (MetaStoreException e) {
      throw new IOException(e);
    }
  }

  public FileInfo getFileInfo(String path) throws IOException {
    try {
      return serverContext.getMetaStore().getFile(path);
    } catch (MetaStoreException e) {
      throw new IOException(e);
    }
  }

  public void reconfigureProperty(String property, String newVal)
      throws ReconfigureException {
    LOG.debug("Received reconfig event: property={} newVal={}",
        property, newVal);
    if (SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY.equals(property)) {
      if (statesUpdaterService != null) {
        throw new ReconfigureException(
            "States update service already been initialized.");
      }

      if (working) {
        initStatesUpdaterService();
      }
    }
  }

  public List<String> getReconfigurableProperties() {
    return Arrays.asList(
        SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY);
  }

  private synchronized void initStatesUpdaterService() {
    try {
      try {
        statesUpdaterService = AbstractServiceFactory
            .createStatesUpdaterService(getContext().getConf(),
                serverContext, serverContext.getMetaStore());
        statesUpdaterService.init();
      } catch (IOException e) {
        statesUpdaterService = null;
        LOG.warn("================================================================");
        LOG.warn("  Failed to create states updater service for: " + e.getMessage());
        LOG.warn("  This may leads to rule/action execution error. The reason why SSM "
            + "does not exit under this condition is some other feature depends on this.");
        LOG.warn("================================================================");
      }

      if (working) {
        try {
          statesUpdaterService.start();
        } catch (IOException e) {
          LOG.error("Failed to start states updater service.", e);
          statesUpdaterService = null;
        }
      }
    } catch (Throwable t) {
      LOG.info("", t);
    }
  }
}
