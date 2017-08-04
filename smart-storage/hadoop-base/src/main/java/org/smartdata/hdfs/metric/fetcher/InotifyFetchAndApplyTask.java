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
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class InotifyFetchAndApplyTask implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(InotifyFetchAndApplyTask.class);

  private final AtomicLong lastId;
  private final InotifyEventApplier applier;
  private DFSInotifyEventInputStream inotifyEventInputStream;

  public InotifyFetchAndApplyTask(DFSClient client, InotifyEventApplier applier, long startId)
      throws IOException {
    this.applier = applier;
    this.lastId = new AtomicLong(startId);
    this.inotifyEventInputStream = client.getInotifyEventStream(startId);
  }

  @Override
  public void run() {
    LOG.trace("InotifyFetchAndApplyTask run at " +  new Date());
    try {
      EventBatch eventBatch = inotifyEventInputStream.poll();
      while (eventBatch != null) {
        this.applier.apply(eventBatch.getEvents());
        this.lastId.getAndSet(eventBatch.getTxid());
        eventBatch = inotifyEventInputStream.poll();
      }
    } catch (Throwable t) {
      LOG.error("Inotify Apply Events error", t);
    }
  }

  public long getLastId() {
    return this.lastId.get();
  }
}
