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
package org.smartdata.hdfs.metric.fetcher;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.FileInfo;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.ingestion.IngestionTask;
import org.smartdata.model.FileInfoBatch;
import org.smartdata.metastore.ingestion.FileStatusIngester;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class NamespaceFetcher {
  private static final Long DEFAULT_INTERVAL = 1L;

  private final ScheduledExecutorService scheduledExecutorService;
  private final long fetchInterval;
  private ScheduledFuture fetchTaskFuture;
  private ScheduledFuture[] consumerFutures;
  private FileStatusIngester[] consumers;
  private IngestionTask ingestionTask;
  private MetaStore metaStore;
  private SmartConf conf;

  public static final Logger LOG =
      LoggerFactory.getLogger(NamespaceFetcher.class);

  public NamespaceFetcher(DFSClient client, MetaStore metaStore, ScheduledExecutorService service) {
    this(client, metaStore, DEFAULT_INTERVAL, service, new SmartConf());
  }

  public NamespaceFetcher(DFSClient client, MetaStore metaStore, ScheduledExecutorService service, SmartConf conf) {
    this(client, metaStore, DEFAULT_INTERVAL, service, conf);
  }

  public NamespaceFetcher(DFSClient client, MetaStore metaStore, long fetchInterval) {
    this(client, metaStore, fetchInterval, null, new SmartConf());
  }

  public NamespaceFetcher(DFSClient client, MetaStore metaStore, long fetchInterval,
      ScheduledExecutorService service, SmartConf conf) {
    this.ingestionTask = new HdfsFetchTask(client, conf);
    int numConsumers = conf.getInt(SmartConfKeys.SMART_NAMESPACE_FETCHER_NUM_CONSUMERS_KEY,
        SmartConfKeys.SMART_NAMESPACE_FETCHER_NUM_CONSUMERS_DEFAULT);
    numConsumers = numConsumers <= 0 ? 1 : numConsumers;
    consumers = new FileStatusIngester[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new FileStatusIngester(metaStore, ingestionTask);
    }
    this.fetchInterval = fetchInterval;
    if (service != null) {
      this.scheduledExecutorService = service;
    } else {
      scheduledExecutorService = Executors.newScheduledThreadPool(numConsumers + 1);
    }
    this.metaStore = metaStore;
    this.conf = conf;
  }

  public void startFetch() throws IOException {
    try {
      metaStore.deleteAllFileInfo();
    } catch (MetaStoreException e) {
      throw new IOException("Error while reset files", e);
    }
    this.fetchTaskFuture = this.scheduledExecutorService.scheduleAtFixedRate(
        ingestionTask, 0, fetchInterval, TimeUnit.MILLISECONDS);

    consumerFutures = new ScheduledFuture[consumers.length];
    for (int i = 0; i < consumers.length; i++) {
      consumerFutures[i] = this.scheduledExecutorService.scheduleAtFixedRate(
          consumers[i], 0, 100, TimeUnit.MILLISECONDS);
    }
    LOG.info("Started.");
  }

  public boolean fetchFinished() {
    return this.ingestionTask.finished();
  }

  public void stop() {
    if (fetchTaskFuture != null) {
      this.fetchTaskFuture.cancel(false);
    }
    if (consumerFutures != null) {
      for (ScheduledFuture f : consumerFutures) {
        if (f != null) {
          f.cancel(false);
        }
      }
    }
  }

  private static class HdfsFetchTask extends IngestionTask {
    private final HdfsFileStatus[] EMPTY_STATUS = new HdfsFileStatus[0];
    private final DFSClient client;
    private final SmartConf conf;
    private List<String> ignoreList = new ArrayList<>();
    private byte[] startAfter = null;
    private final byte[] empty = HdfsFileStatus.EMPTY_NAME;

    public HdfsFetchTask(DFSClient client, SmartConf conf) {
      super();
      this.client = client;
      this.conf = conf;
      String configString = conf.get(SmartConfKeys.SMART_IGNORE_DIRS_KEY);
      defaultBatchSize = conf.getInt(SmartConfKeys
              .SMART_NAMESPACE_FETCHER_BATCH_KEY,
          SmartConfKeys.SMART_NAMESPACE_FETCHER_BATCH_DEFAULT);
      if (configString != null) {
        configString = configString.trim();
        if (!configString.equals("")) {
          //only when parent dir is not ignored we run the follow code
          ignoreList = Arrays.asList(configString.split(","));
          for (int i = 0; i < ignoreList.size(); i++) {
            if (!ignoreList.get(i).endsWith("/")) {
              ignoreList.set(i, ignoreList.get(i).concat("/"));
            }
          }
        }
      }
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        long curr = System.currentTimeMillis();
        if (curr - lastUpdateTime >= 2000) {
          LOG.debug(String.format(
              "%d sec, numDirectories = %d, numFiles = %d, batchsInqueue = %d",
              (curr - startTime) / 1000,
              numDirectoriesFetched, numFilesFetched, batches.size()));
          lastUpdateTime = curr;
        }
      }

      String parent = deque.pollFirst();
      if (parent == null) { // BFS finished
        if (currentBatch.actualSize() > 0) {
          try {
            this.batches.put(currentBatch);
          } catch (InterruptedException e) {
            LOG.error("Current batch actual size = "
                + currentBatch.actualSize(), e);
          }
          this.currentBatch = new FileInfoBatch(defaultBatchSize);
        }

        if (this.batches.isEmpty()) {
          if (!this.isFinished) {
            this.isFinished = true;
            long curr = System.currentTimeMillis();
            LOG.info(String.format(
                "Finished fetch Namespace! %d secs used, numDirs = %d, numFiles = %d",
                (curr - startTime) / 1000,
                numDirectoriesFetched, numFilesFetched));
          }
        }
        return;
      }
      String tmpParent = parent;
      if (!tmpParent.endsWith("/")) {
        tmpParent = tmpParent.concat("/");
      }
      for (int i = 0; i < ignoreList.size(); i++) {

      if (startAfter == null) {
        String tmpParent = parent;
        if (!tmpParent.endsWith("/")) {
          tmpParent = tmpParent.concat("/");
        }
        for (int i = 0; i < ignoreList.size(); i++) {
          if (ignoreList.get(i).equals(tmpParent)) {
            return;
          }
        }
      }

      try {
        HdfsFileStatus status = client.getFileInfo(parent);
        if (status != null && status.isDir()) {
          FileInfo internal = convertToFileInfo(status, "");
          internal.setPath(parent);
          this.addFileStatus(internal);
          numDirectoriesFetched++;
          HdfsFileStatus[] children = this.listStatus(parent);
          for (HdfsFileStatus child : children) {
            if (child.isDir()) {
              this.deque.add(child.getFullName(parent));
            } else {
              this.addFileStatus(convertToFileInfo(child, parent));
              numFilesFetched++;
            }
          }
        }
      } catch (IOException | InterruptedException e) {
        LOG.error("Totally, numDirectoriesFetched = " + numDirectoriesFetched
            + ", numFilesFetched = " + numFilesFetched
            + ". Parent = " + parent, e);
      }

    }

    /**
     *  Code copy form {@link org.apache.hadoop.fs.Hdfs}
     */
    private HdfsFileStatus[] listStatus(String src) throws IOException {
      DirectoryListing thisListing = client.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);
      if (thisListing == null) {
        // the directory does not exist
        return EMPTY_STATUS;
      }
      HdfsFileStatus[] partialListing = thisListing.getPartialListing();
      if (!thisListing.hasMore()) {
        // got all entries of the directory
        return partialListing;
      }
      // The directory size is too big that it needs to fetch more
      // estimate the total number of entries in the directory
      int totalNumEntries =
        partialListing.length + thisListing.getRemainingEntries();
      ArrayList<HdfsFileStatus> listing = new ArrayList<>(totalNumEntries);
      Collections.addAll(listing, partialListing);

      // now fetch more entries
      do {
        thisListing = client.listPaths(src, thisListing.getLastName());

        if (thisListing == null) {
          // the directory is deleted
          listing.toArray(new HdfsFileStatus[listing.size()]);
        }

        partialListing = thisListing.getPartialListing();
        Collections.addAll(listing, partialListing);
      } while (thisListing.hasMore());

      return listing.toArray(new HdfsFileStatus[listing.size()]);
    }

    private FileInfo convertToFileInfo(HdfsFileStatus status, String parent) {
      FileInfo fileInfo = new FileInfo(
          status.getFullName(parent),
          status.getFileId(),
          status.getLen(),
          status.isDir(),
          status.getReplication(),
          status.getBlockSize(),
          status.getModificationTime(),
          status.getAccessTime(),
          status.getPermission().toShort(),
          status.getOwner(),
          status.getGroup(),
          status.getStoragePolicy());
      return fileInfo;
    }
  }
}
