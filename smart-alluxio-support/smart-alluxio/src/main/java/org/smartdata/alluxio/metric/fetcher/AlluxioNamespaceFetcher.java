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
package org.smartdata.alluxio.metric.fetcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.ingestion.IngestionTask;
import org.smartdata.model.FileInfo;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.FileInfoBatch;
import org.smartdata.metastore.ingestion.FileStatusIngester;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AlluxioNamespaceFetcher {
  public static final Long DEFAULT_INTERVAL = 1000L;

  private final ScheduledExecutorService scheduledExecutorService;
  private final long fetchInterval;
  private ScheduledFuture fetchTaskFuture;
  private ScheduledFuture consumerFuture;
  private FileStatusIngester consumer;
  private AlluxioFetchTask fetchTask;

  public static final Logger LOG =
      LoggerFactory.getLogger(AlluxioNamespaceFetcher.class);

  public AlluxioNamespaceFetcher(FileSystem fs, MetaStore metaStore, long fetchInterval,
      ScheduledExecutorService service) {
    this.fetchTask = new AlluxioFetchTask(fs);
    this.consumer = new FileStatusIngester(metaStore, fetchTask);
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

  private static class AlluxioFetchTask extends IngestionTask {

    private final FileSystem fs;

    private long lastUpdateTime = System.currentTimeMillis();
    private long startTime = lastUpdateTime;

    public AlluxioFetchTask(FileSystem fs) {
      super();
      this.fs = fs;
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

      try {
        URIStatus status = fs.getStatus(new AlluxioURI(parent));
        if (status != null && status.isFolder()) {
          List<URIStatus> children = fs.listStatus(new AlluxioURI(parent));
          FileInfo fileInfo = convertToFileInfo(status);
          this.addFileStatus(fileInfo);
          numDirectoriesFetched++;
          for (URIStatus child : children) {
            if (child.isFolder()) {
              this.deque.add(child.getPath());
            } else {
              this.addFileStatus(convertToFileInfo(child));
              numFilesFetched++;
            }
          }
        }
      } catch (IOException | InterruptedException | AlluxioException e) {
        LOG.error("Totally, numDirectoriesFetched = " + numDirectoriesFetched
            + ", numFilesFetched = " + numFilesFetched
            + ". Parent = " + parent, e);
      }
    }

    private FileInfo convertToFileInfo(URIStatus status) {
      FileInfo fileInfo = new FileInfo(
          status.getPath(),
          status.getFileId(),
          status.getLength(),
          status.isFolder(),
          (short)1,
          status.getBlockSizeBytes(),
          status.getLastModificationTimeMs(),
          status.getCreationTimeMs(),
          (short) status.getMode(),
          status.getOwner(),
          status.getGroup(),
          (byte) 0);
      return fileInfo;
    }
  }
}
