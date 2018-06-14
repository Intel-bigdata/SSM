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

import alluxio.master.journal.JournalReader;
import alluxio.master.journal.ufs.AlluxioJournalUtil;
import alluxio.proto.journal.Journal.JournalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.conf.SmartConf;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.SystemInfo;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class AlluxioEntryFetchAndApplyTask implements Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(AlluxioEntryFetchAndApplyTask.class);

  private final AtomicLong lastSn;
  private final MetaStore metaStore;
  private final AlluxioEntryApplier entryApplier;
  private JournalReader journalReader;
  
  public AlluxioEntryFetchAndApplyTask(SmartConf conf, MetaStore metaStore, AlluxioEntryApplier entryApplier, long startSn) {
    this.metaStore = metaStore;
    this.entryApplier = entryApplier;
    this.lastSn = new AtomicLong(startSn);
    this.journalReader = AlluxioJournalUtil.getJournalReaderFromSn(conf, startSn);
  }


  @Override
  public void run() {
    LOG.trace("AlluxioEntryFetchAndApplyTask run at " +  new Date());
    try {
      JournalEntry journalEntry = journalReader.read();
      while (journalEntry != null) {
        entryApplier.apply(journalEntry);
        lastSn.getAndSet(journalEntry.getSequenceNumber());
        metaStore.updateAndInsertIfNotExist(
            new SystemInfo(
                SmartConstants.SMART_ALLUXIO_LAST_ENTRY_SN, String.valueOf(lastSn.get())));
        journalEntry = journalReader.read();
      }
    } catch (Throwable t) {
      LOG.error("Alluxio Entry Apply Events error", t);
    }
  }

  public long getLastSn() {
    return this.lastSn.get();
  }
}
