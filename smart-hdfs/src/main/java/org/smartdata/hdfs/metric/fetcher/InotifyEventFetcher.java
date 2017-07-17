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

import com.squareup.tape.QueueFile;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class InotifyEventFetcher {
  private final DFSClient client;
  private final NamespaceFetcher nameSpaceFetcher;
  private final ScheduledExecutorService scheduledExecutorService;
  private final InotifyEventApplier applier;
  private ScheduledFuture inotifyFetchFuture;
  private ScheduledFuture fetchAndApplyFuture;
  private EventApplyTask eventApplyTask;
  private java.io.File inotifyFile;
  private QueueFile queueFile;
  public static final Logger LOG =
      LoggerFactory.getLogger(InotifyEventFetcher.class);

  public InotifyEventFetcher(DFSClient client, MetaStore metaStore,
      ScheduledExecutorService service) {
    this(client, metaStore, service, new InotifyEventApplier(metaStore, client));
  }

  public InotifyEventFetcher(DFSClient client, MetaStore metaStore,
      ScheduledExecutorService service, InotifyEventApplier applier) {
    this.client = client;
    this.applier = applier;
    this.scheduledExecutorService = service;
    this.nameSpaceFetcher = new NamespaceFetcher(client, metaStore, service);
  }

  public void start() throws IOException {
    this.inotifyFile = new File("/tmp/inotify" + new Random().nextLong());
    this.queueFile = new QueueFile(inotifyFile);
    long startId = this.client.getNamenode().getCurrentEditLogTxid();
    LOG.info("Start fetching namespace with current edit log txid = " + startId);
    this.nameSpaceFetcher.startFetch();
    this.inotifyFetchFuture = scheduledExecutorService.scheduleAtFixedRate(
        new InotifyFetchTask(queueFile, client, startId), 0, 100, TimeUnit.MILLISECONDS);
    this.eventApplyTask = new EventApplyTask(nameSpaceFetcher, applier, queueFile, startId);

    LOG.info("Start apply iNotify events.");
    eventApplyTask.start();

    try {
      this.waitNameSpaceFetcherFinished();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOG.info("Name space fetch finished.");
  }

  private void waitNameSpaceFetcherFinished() throws InterruptedException, IOException {
    eventApplyTask.join();

    long lastId = eventApplyTask.getLastId();
    this.inotifyFetchFuture.cancel(false);
    this.nameSpaceFetcher.stop();
    this.queueFile.close();
    InotifyFetchAndApplyTask fetchAndApplyTask =
      new InotifyFetchAndApplyTask(client, applier, lastId);
    this.fetchAndApplyFuture = scheduledExecutorService.scheduleAtFixedRate(
      fetchAndApplyTask, 0, 100, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    if (inotifyFile != null) {
      this.inotifyFile.delete();
    }
    if (inotifyFetchFuture != null) {
      this.inotifyFetchFuture.cancel(false);
    }
    if (this.fetchAndApplyFuture != null){
      this.fetchAndApplyFuture.cancel(false);
    }
  }

  private static class InotifyFetchTask implements Runnable {
    private final QueueFile queueFile;
    private DFSInotifyEventInputStream inotifyEventInputStream;

    public InotifyFetchTask(QueueFile queueFile, DFSClient client, long startId) throws IOException {
      this.queueFile = queueFile;
      this.inotifyEventInputStream = client.getInotifyEventStream(startId);
    }

    @Override
    public void run() {
      try {
        EventBatch eventBatch = inotifyEventInputStream.poll();
        while (eventBatch != null) {
          this.queueFile.add(EventBatchSerializer.serialize(eventBatch));
          eventBatch = inotifyEventInputStream.poll();
        }
      } catch (IOException | MissingEventsException e) {
        LOG.error("Inotify enqueue error", e);
      }
    }
  }

  private static class EventApplyTask extends Thread {
    private final NamespaceFetcher namespaceFetcher;
    private final InotifyEventApplier applier;
    private final QueueFile queueFile;
    private long lastId;

    public EventApplyTask(NamespaceFetcher namespaceFetcher, InotifyEventApplier applier,
        QueueFile queueFile, long lastId) {
      this.namespaceFetcher = namespaceFetcher;
      this.queueFile = queueFile;
      this.applier = applier;
      this.lastId = lastId;
    }

    @Override
    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          if (!namespaceFetcher.fetchFinished()) {
            Thread.sleep(100);
          } else {
            while (!queueFile.isEmpty()) {
              EventBatch batch = EventBatchSerializer.deserialize(queueFile.peek());
              queueFile.remove();
              this.applier.apply(batch.getEvents());
              this.lastId = batch.getTxid();
            }
            break;
          }
        }
      } catch (InterruptedException | IOException | MetaStoreException e) {
        LOG.error("Inotify dequeue error", e);
      }
    }

    public long getLastId() {
      return this.lastId;
    }
  }
}
