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

import com.squareup.tape.QueueFile;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.utils.EventBatchSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class InotifyEventFetcher {
  private final DFSClient client;
  private final DBAdapter adapter;
  private final NamespaceFetcher nameSpaceFetcher;
  private final ScheduledExecutorService scheduledExecutorService;
  private final InotifyEventApplier applier;
  private ScheduledFuture inotifyFetchFuture;
  private ScheduledFuture fetchAndApplyFuture;
  private java.io.File inotifyFile;
  private QueueFile queueFile;

  public InotifyEventFetcher(DFSClient client, DBAdapter adapter,
      ScheduledExecutorService service, InotifyEventApplier applier) {
    this.client = client;
    this.adapter = adapter;
    this.applier = applier;
    this.scheduledExecutorService = service;
    this.nameSpaceFetcher = new NamespaceFetcher(client, adapter, service);
  }

  public void start() throws IOException, InterruptedException {
    this.inotifyFile = new File("/tmp/inotify" + new Random().nextLong());
    this.queueFile = new QueueFile(inotifyFile);
    long startId = this.client.getNamenode().getCurrentEditLogTxid();
    this.nameSpaceFetcher.startFetch();
    this.inotifyFetchFuture = scheduledExecutorService.scheduleAtFixedRate(
        new InotifyFetchTask(queueFile, client, startId), 0, 100, TimeUnit.MILLISECONDS);
    EventApplyTask eventApplyTask = new EventApplyTask(nameSpaceFetcher, applier, queueFile);
    eventApplyTask.start();
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
    this.inotifyFile.delete();
    this.fetchAndApplyFuture.cancel(false);
  }

  private static class InotifyFetchTask implements Runnable {
    private final QueueFile queueFile;
    private DFSInotifyEventInputStream inotifyEventInputStream;

    public InotifyFetchTask(QueueFile queueFile, DFSClient client, long startId) throws IOException {
      this.queueFile = queueFile;
      this.inotifyEventInputStream = client.getInotifyEventStream();
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
        e.printStackTrace();
      }
    }
  }

  private static class EventApplyTask extends Thread {
    private final NamespaceFetcher namespaceFetcher;
    private final InotifyEventApplier applier;
    private final QueueFile queueFile;
    private long lastId;

    public EventApplyTask(NamespaceFetcher namespaceFetcher, InotifyEventApplier applier,
        QueueFile queueFile) {
      this.namespaceFetcher = namespaceFetcher;
      this.queueFile = queueFile;
      this.applier = applier;
      this.lastId = -1;
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
      } catch (InterruptedException | IOException e) {
        e.printStackTrace();
      }
    }

    public long getLastId() {
      return this.lastId;
    }
  }
}
