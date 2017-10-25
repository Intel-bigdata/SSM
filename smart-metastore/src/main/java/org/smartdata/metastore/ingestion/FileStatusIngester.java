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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.FileInfo;
import org.smartdata.model.FileInfoBatch;

public class FileStatusIngester implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(FileStatusIngester.class);

  private final MetaStore dbAdapter;
  private final IngestionTask ingestionTask;
  private long startTime = System.currentTimeMillis();
  private long lastUpdateTime = startTime;

  public FileStatusIngester(MetaStore dbAdapter, IngestionTask ingestionTask) {
    this.dbAdapter = dbAdapter;
    this.ingestionTask = ingestionTask;
  }

  @Override
  public void run() {
    FileInfoBatch batch = ingestionTask.pollBatch();
    try {
      if (batch != null) {
        FileInfo[] statuses = batch.getFileInfos();
        if (statuses.length == batch.actualSize()) {
          this.dbAdapter.insertFiles(batch.getFileInfos());
          IngestionTask.numPersisted += statuses.length;
        } else {
          FileInfo[] actual = new FileInfo[batch.actualSize()];
          System.arraycopy(statuses, 0, actual, 0, batch.actualSize());
          this.dbAdapter.insertFiles(actual);
          IngestionTask.numPersisted += actual.length;
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
        long total = IngestionTask.numDirectoriesFetched + IngestionTask.numFilesFetched;
        if (total > 0) {
          LOG.debug(String.format(
              "%d sec, %%%d persisted into database",
              (curr - startTime) / 1000, IngestionTask.numPersisted * 100 / total));
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
