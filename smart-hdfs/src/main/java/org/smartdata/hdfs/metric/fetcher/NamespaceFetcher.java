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
import org.smartdata.model.FileInfo;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.fetcher.FetchTask;
import org.smartdata.metastore.fetcher.FileInfoBatch;
import org.smartdata.metastore.fetcher.FileStatusConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class NamespaceFetcher {
  private static final Long DEFAULT_INTERVAL = 100L;

  private final ScheduledExecutorService scheduledExecutorService;
  private final long fetchInterval;
  private ScheduledFuture fetchTaskFuture;
  private ScheduledFuture consumerFuture;
  private FileStatusConsumer consumer;
  private FetchTask fetchTask;

  public static final Logger LOG =
      LoggerFactory.getLogger(NamespaceFetcher.class);

  public NamespaceFetcher(DFSClient client, MetaStore metaStore) {
    this(client, metaStore, DEFAULT_INTERVAL);
  }

  public NamespaceFetcher(DFSClient client, MetaStore metaStore, ScheduledExecutorService service) {
    this(client, metaStore, DEFAULT_INTERVAL, service);
  }

  public NamespaceFetcher(DFSClient client, MetaStore metaStore, long fetchInterval) {
    this(client, metaStore, fetchInterval, Executors.newSingleThreadScheduledExecutor());
  }

  public NamespaceFetcher(DFSClient client, MetaStore metaStore, long fetchInterval,
      ScheduledExecutorService service) {
    this.fetchTask = new HdfsFetchTask(client);
    this.consumer = new FileStatusConsumer(metaStore, fetchTask);
    this.fetchInterval = fetchInterval;
    this.scheduledExecutorService = service;
  }

  public void startFetch() throws IOException {
    this.fetchTaskFuture = this.scheduledExecutorService.scheduleAtFixedRate(
        fetchTask, 0, fetchInterval, TimeUnit.MILLISECONDS);
    this.consumerFuture = this.scheduledExecutorService.scheduleAtFixedRate(
        consumer, 0, 100, TimeUnit.MILLISECONDS);
    LOG.info("Started.");
  }

  public boolean fetchFinished() {
    return this.fetchTask.finished();
  }

  public void stop() {
    if (fetchTaskFuture != null) {
      this.fetchTaskFuture.cancel(false);
    }
    if (consumerFuture != null) {
      this.consumerFuture.cancel(false);
    }
  }

  private static class HdfsFetchTask extends FetchTask {
    private final HdfsFileStatus[] EMPTY_STATUS = new HdfsFileStatus[0];
    private final DFSClient client;
    public HdfsFetchTask(DFSClient client) {
      super();
      this.client = client;
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
          this.currentBatch = new FileInfoBatch(DEFAULT_BATCH_SIZE);
        }

        if (this.batches.isEmpty()) {
          this.isFinished = true;
          long curr = System.currentTimeMillis();
          LOG.info(String.format(
              "Finished fetch Namespace! %d secs used, numDirs = %d, numFiles = %d",
              (curr - startTime) / 1000,
              numDirectoriesFetched, numFilesFetched));
        }
        return;
      }

      try {
        HdfsFileStatus status = client.getFileInfo(parent);
        if (status != null && status.isDir()) {
          FileInfo internal = convertToFileInfo(status,"");
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
