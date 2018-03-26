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
package org.smartdata.metastore.ingestion;

import org.smartdata.model.FileInfo;
import org.smartdata.model.FileInfoBatch;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

public abstract class IngestionTask implements Runnable {
  public static AtomicLong numFilesFetched = new AtomicLong(0);
  public static AtomicLong numDirectoriesFetched = new AtomicLong(0);
  public static AtomicLong numPersisted = new AtomicLong(0);

  protected int defaultBatchSize = 20;
  protected int maxPendingBatches = 80;

  protected static final String ROOT = "/";
  // Deque for Breadth-First-Search
  protected static LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<>();
  // Queue for outer-consumer to fetch file status
  protected static LinkedBlockingDeque<FileInfoBatch> batches = new LinkedBlockingDeque<>();
  protected FileInfoBatch currentBatch;
  protected static volatile boolean isFinished = false;

  protected long lastUpdateTime = System.currentTimeMillis();
  protected long startTime = lastUpdateTime;

  public IngestionTask() {
    this.currentBatch = new FileInfoBatch(defaultBatchSize);
    if (deque.isEmpty()) {
      this.deque.add(ROOT);
    }
  }
  public static boolean finished() {
    return isFinished;
  }

  public static FileInfoBatch pollBatch() {
    return batches.poll();
  }

  public void addFileStatus(FileInfo status) throws InterruptedException {
    this.currentBatch.add(status);
    if (this.currentBatch.isFull()) {
      this.batches.put(currentBatch);
      this.currentBatch = new FileInfoBatch(defaultBatchSize);
    }
  }
}
