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
import org.smartdata.common.models.FileStatusInternal;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class NamespaceFetcher {
  private static final Long DEFAULT_INTERVAL = 1000L;

  private final ScheduledExecutorService scheduledExecutorService;
  private final long fetchInterval;
  private ScheduledFuture fetchTaskFuture;
  private ScheduledFuture consumerFuture;
  private FileStatusConsumer consumer;
  private FetchTask fetchTask;

  private static long numFilesFetched = 0L;
  private static long numDirectoriesFetched = 0L;
  private static long numPersisted = 0L;

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
    this.fetchTask = new FetchTask(client);
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

  private static class FetchTask implements Runnable {
    private final static int DEFAULT_BATCH_SIZE = 20;
    private final static String ROOT = "/";
    private final HdfsFileStatus[] EMPTY_STATUS = new HdfsFileStatus[0];
    private final DFSClient client;
    // Deque for Breadth-First-Search
    private ArrayDeque<String> deque;
    // Queue for outer-consumer to fetch file status
    private LinkedBlockingDeque<FileStatusInternalBatch> batches;
    private FileStatusInternalBatch currentBatch;
    private volatile boolean isFinished = false;

    private long lastUpdateTime = System.currentTimeMillis();
    private long startTime = lastUpdateTime;

    public FetchTask(DFSClient client) {
      this.deque = new ArrayDeque<>();
      this.batches = new LinkedBlockingDeque<>();
      this.currentBatch = new FileStatusInternalBatch(DEFAULT_BATCH_SIZE);
      this.client = client;
      this.deque.add(ROOT);
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
          this.currentBatch = new FileStatusInternalBatch(DEFAULT_BATCH_SIZE);
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
          FileStatusInternal internal = new FileStatusInternal(status);
          internal.setPath(parent);
          this.addFileStatus(internal);
          numDirectoriesFetched++;
          HdfsFileStatus[] children = this.listStatus(parent);
          for (HdfsFileStatus child : children) {
            if (child.isDir()) {
              this.deque.add(child.getFullName(parent));
            } else {
              this.addFileStatus(new FileStatusInternal(child, parent));
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

    public boolean finished() {
      return this.isFinished;
    }

    public FileStatusInternalBatch pollBatch() {
      return this.batches.poll();
    }

    public void addFileStatus(FileStatusInternal status) throws InterruptedException {
      this.currentBatch.add(status);
      if (this.currentBatch.isFull()) {
        this.batches.put(currentBatch);
        this.currentBatch = new FileStatusInternalBatch(DEFAULT_BATCH_SIZE);
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
  }

  private static class FileStatusConsumer implements Runnable {
    private final MetaStore dbAdapter;
    private final FetchTask fetchTask;
    private long startTime = System.currentTimeMillis();
    private long lastUpdateTime = startTime;

    protected FileStatusConsumer(MetaStore dbAdapter, FetchTask fetchTask) {
      this.dbAdapter = dbAdapter;
      this.fetchTask = fetchTask;
    }

    @Override
    public void run() {
      FileStatusInternalBatch batch = fetchTask.pollBatch();
      try {
        if (batch != null) {
          FileStatusInternal[] statuses = batch.getFileStatuses();
          if (statuses.length == batch.actualSize()) {
            this.dbAdapter.insertFiles(batch.getFileStatuses());
            numPersisted += statuses.length;
          } else {
            FileStatusInternal[] actual = new FileStatusInternal[batch.actualSize()];
            System.arraycopy(statuses, 0, actual, 0, batch.actualSize());
            this.dbAdapter.insertFiles(actual);
            numPersisted += actual.length;
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug(batch.actualSize() + " files insert into table 'files'.");
          }
        }
      } catch (MetaStoreException e) {
        // TODO: handle this issue
        LOG.error("Consumer error");
      }

      if (LOG.isDebugEnabled()) {
        long curr = System.currentTimeMillis();
        if (curr - lastUpdateTime >= 2000) {
          long total = numDirectoriesFetched + numFilesFetched;
          if (total > 0) {
            LOG.debug(String.format(
                "%d sec, %%%d persisted into database",
                (curr - startTime) / 1000, numPersisted * 100 / total));
          } else {
            LOG.debug(String.format(
                "%d sec, %%0 persisted into database",
                (curr - startTime) / 1000));
          }
          lastUpdateTime = curr;
        }
      }
    }
  }

  private static class FileStatusInternalBatch {
    private FileStatusInternal[] fileStatuses;
    private final int batchSize;
    private int index;

    public FileStatusInternalBatch(int batchSize) {
      this.batchSize = batchSize;
      this.fileStatuses = new FileStatusInternal[batchSize];
      this.index = 0;
    }

    public void add(FileStatusInternal status) {
      this.fileStatuses[index] = status;
      index += 1;
    }

    public boolean isFull() {
      return index == batchSize;
    }

    public int actualSize() {
      return this.index;
    }

    public FileStatusInternal[] getFileStatuses() {
      return this.fileStatuses;
    }
  }
}
