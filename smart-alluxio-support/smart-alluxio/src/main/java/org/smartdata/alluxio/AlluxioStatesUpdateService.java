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

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.alluxio.metric.fetcher.AlluxioEntryFetcher;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.StatesUpdateService;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
/**
 * Polls metrics and events from Alluxio Server
 */
public class AlluxioStatesUpdateService extends StatesUpdateService {
  private static final String ALLUXIO_MOVER_ID_PATH = "/system/alluxio-mover.id";
  private volatile boolean inSafeMode;
  private FileSystem alluxioFs;
  private ScheduledExecutorService executorService;
  private AlluxioEntryFetcher alluxioEntryFetcher;
  private FileOutStream moverIdOutStream;

  public static final Logger LOG =
      LoggerFactory.getLogger(AlluxioStatesUpdateService.class);

  public AlluxioStatesUpdateService(SmartContext context, MetaStore metaStore) {
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
    this.alluxioFs = AlluxioUtil.getAlluxioFs(context);
    this.moverIdOutStream = checkAndMarkRunning(alluxioFs);
    this.executorService = Executors.newScheduledThreadPool(4);
    this.alluxioEntryFetcher = new AlluxioEntryFetcher(alluxioFs, metaStore,
        executorService, new EntryFetchFinishedCallBack(), context.getConf());
    LOG.info("Initialized.");
  }

  private class EntryFetchFinishedCallBack implements Callable<Object> {
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
    this.alluxioEntryFetcher.start();
    LOG.info("Started. ");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");
    if (moverIdOutStream != null) {
      try {
        moverIdOutStream.close();
      } catch (IOException e) {
        LOG.debug("Close alluxio 'mover' ID output stream error", e);
      }
    }
    if (alluxioEntryFetcher != null) {
      alluxioEntryFetcher.stop();
    }
    LOG.info("Stopped.");
  }

  private FileOutStream checkAndMarkRunning(FileSystem fs) throws IOException {
    AlluxioURI moverIdPath = new AlluxioURI(ALLUXIO_MOVER_ID_PATH);
    try {
      if (fs.exists(moverIdPath)) {
        // Alluxio does not support append operation (ALLUXIO-25), here just delete it
        fs.delete(moverIdPath, DeleteOptions.defaults().setRecursive(true));
      }
      CreateFileOptions options = CreateFileOptions.defaults().setWriteType(
          WriteType.MUST_CACHE);
      FileOutStream fos = fs.createFile(moverIdPath, options);
      fos.write(InetAddress.getLocalHost().getHostName().getBytes());
      fos.flush();
      return fos;
    } catch (IOException | AlluxioException e) {
      LOG.error("Unable to lock alluxio 'mover', please stop alluxio 'mover' first.");
      throw new IOException(e.getMessage());
    }
  }

}
