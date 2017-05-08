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
package org.apache.hadoop.ssm.fetcher;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.FileStatusInternal;

import java.io.IOException;
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

  public NamespaceFetcher(DFSClient client, DBAdapter adapter) {
    this(client, adapter, DEFAULT_INTERVAL);
  }

  public NamespaceFetcher(DFSClient client, DBAdapter adapter, ScheduledExecutorService service) {
    this(client, adapter, DEFAULT_INTERVAL, service);
  }

  public NamespaceFetcher(DFSClient client, DBAdapter adapter, long fetchInterval) {
    this(client, adapter, fetchInterval, Executors.newSingleThreadScheduledExecutor());
  }

  public NamespaceFetcher(DFSClient client, DBAdapter adapter, long fetchInterval,
      ScheduledExecutorService service) {
    this.fetchTask = new FetchTask(client);
    this.consumer = new FileStatusConsumer(adapter, fetchTask);
    this.fetchInterval = fetchInterval;
    this.scheduledExecutorService = service;
  }

  public void startFetch() throws IOException {
    this.fetchTaskFuture = this.scheduledExecutorService.scheduleAtFixedRate(
        fetchTask, 0, fetchInterval, TimeUnit.MILLISECONDS);
    this.consumerFuture = this.scheduledExecutorService.scheduleAtFixedRate(
        consumer, 0, 100, TimeUnit.MILLISECONDS);
  }

  public boolean fetchFinished() {
    return this.fetchTask.finished();
  }

  public void stop() {
    this.fetchTaskFuture.cancel(false);
    this.consumerFuture.cancel(false);
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

    public FetchTask(DFSClient client) {
      this.deque = new ArrayDeque<>();
      this.batches = new LinkedBlockingDeque<>();
      this.currentBatch = new FileStatusInternalBatch(DEFAULT_BATCH_SIZE);
      this.client = client;
      this.deque.add(ROOT);
    }

    @Override
    public void run() {
      String parent = deque.pollFirst();
      if (parent == null) { // BFS finished
        if (currentBatch.actualSize() > 0) {
          this.batches.add(currentBatch);
          this.currentBatch = new FileStatusInternalBatch(DEFAULT_BATCH_SIZE);
        }
        return;
      }
      try {
        HdfsFileStatus status = client.getFileInfo(parent);
        if (status != null && status.isDir()) {
          FileStatusInternal internal = new FileStatusInternal(status);
          internal.setPath(parent);
          this.addFileStatus(internal);
          HdfsFileStatus[] children = this.listStatus(parent);
          for (HdfsFileStatus child : children) {
            if (child.isDir()) {
              this.deque.add(child.getFullName(parent));
            } else {
              this.addFileStatus(new FileStatusInternal(child, parent));
            }
          }
        }
        if (this.deque.isEmpty() && this.batches.isEmpty()) {
          this.isFinished = true;
        }
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
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
    private final DBAdapter dbAdapter;
    private final FetchTask fetchTask;

    protected FileStatusConsumer(DBAdapter dbAdapter, FetchTask fetchTask) {
      this.dbAdapter = dbAdapter;
      this.fetchTask = fetchTask;
    }

    @Override
    public void run() {
      FileStatusInternalBatch batch = fetchTask.pollBatch();
      if (batch != null) {
        FileStatusInternal[] statuses = batch.getFileStatuses();
        if (statuses.length == batch.actualSize()) {
          this.dbAdapter.insertFiles(batch.getFileStatuses());
        } else {
          FileStatusInternal[] actual = new FileStatusInternal[batch.actualSize()];
          System.arraycopy(statuses, 0, actual, 0, batch.actualSize());
          this.dbAdapter.insertFiles(actual);
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
