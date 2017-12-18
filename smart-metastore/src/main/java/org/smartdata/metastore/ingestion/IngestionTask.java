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

import java.util.ArrayDeque;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class IngestionTask implements Runnable {
  public static long numFilesFetched = 0L;
  public static long numDirectoriesFetched = 0L;
  public static long numPersisted = 0L;

  protected int defaultBatchSize = 20;

  protected static final String ROOT = "/";
  // Deque for Breadth-First-Search
  protected ArrayDeque<String> deque;
  // Queue for outer-consumer to fetch file status
  protected LinkedBlockingDeque<FileInfoBatch> batches;
  protected FileInfoBatch currentBatch;
  protected volatile boolean isFinished = false;

  protected long lastUpdateTime = System.currentTimeMillis();
  protected long startTime = lastUpdateTime;

  public IngestionTask() {
    this.deque = new ArrayDeque<>();
    this.batches = new LinkedBlockingDeque<>();
    this.currentBatch = new FileInfoBatch(defaultBatchSize);
    this.deque.add(ROOT);
  }
  public boolean finished() {
    return this.isFinished;
  }

  public FileInfoBatch pollBatch() {
    return this.batches.poll();
  }

  public void addFileStatus(FileInfo status) throws InterruptedException {
    this.currentBatch.add(status);
    if (this.currentBatch.isFull()) {
      this.batches.put(currentBatch);
      this.currentBatch = new FileInfoBatch(defaultBatchSize);
    }
  }
}
