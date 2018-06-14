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

import alluxio.client.file.FileSystem;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.ufs.AlluxioJournalUtil;
import alluxio.proto.journal.Journal.JournalEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.*;
import com.squareup.tape.QueueFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.SystemInfo;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AlluxioEntryFetcher {

  public static final Logger LOG = LoggerFactory.getLogger(AlluxioEntryFetcher.class);

  private final FileSystem fileSystem;
  private final AlluxioNamespaceFetcher alluxioNamespaceFetcher;
  private final ScheduledExecutorService scheduledExecutorService;
  private final AlluxioEntryApplier alluxioEntryApplier;
  private final MetaStore metaStore;
  private Callable finishedCallback;
  private ScheduledFuture entryFetchFuture;
  private ScheduledFuture entryFetchAndApplyFuture;
  private AlluxioEntryApplyTask entryApplyTask;
  private File entryInotifyFile;
  private QueueFile entryQueueFile;
  private SmartConf conf;

  public AlluxioEntryFetcher(FileSystem fileSystem, MetaStore metaStore, ScheduledExecutorService scheduledExecutorService,
                             Callable finishedCallback) {
    this(fileSystem, metaStore, scheduledExecutorService,
        new AlluxioEntryApplier(metaStore, fileSystem), finishedCallback, new SmartConf());
  }

  public AlluxioEntryFetcher(FileSystem fileSystem, MetaStore metaStore, ScheduledExecutorService scheduledExecutorService,
                             Callable finishedCallback, SmartConf conf) {
    this(fileSystem, metaStore, scheduledExecutorService,
        new AlluxioEntryApplier(metaStore, fileSystem), finishedCallback, conf);
  }

  public AlluxioEntryFetcher(FileSystem fileSystem, MetaStore metaStore, ScheduledExecutorService scheduledExecutorService,
                             AlluxioEntryApplier alluxioEntryApplier, Callable finishedCallback, SmartConf conf) {
    this.fileSystem = fileSystem;
    this.scheduledExecutorService = scheduledExecutorService;
    this.alluxioEntryApplier = alluxioEntryApplier;
    this.metaStore = metaStore;
    this.finishedCallback = finishedCallback;
    this.conf = conf;
    this.alluxioNamespaceFetcher = new AlluxioNamespaceFetcher(fileSystem, metaStore, scheduledExecutorService);
  }

  public void start() throws IOException {
    Long lastSn = getLastSeqNum();
    if (lastSn != null && lastSn != -1 && canReadFromLastSeqNum(lastSn)) {
      startFromLastSeqNum(lastSn);
    } else {
      startWithFetchingAlluxioNameSpace();
    }
  }

  @VisibleForTesting
  boolean canReadFromLastSeqNum(Long lastSn) {
    try {
      if (AlluxioJournalUtil.getCurrentSeqNum(conf) == lastSn) {
        return true;
      }
      JournalReader reader = AlluxioJournalUtil.getJournalReaderFromSn(conf, lastSn);
      JournalEntry entry = reader.read();
      return entry != null;
    } catch (Exception e) {
      return false;
    }
  }

  private Long getLastSeqNum() {
    try {
      SystemInfo info =
          metaStore.getSystemInfoByProperty(SmartConstants.SMART_ALLUXIO_LAST_ENTRY_SN);
      return info != null ? Long.parseLong(info.getValue()) : -1L;
    } catch (MetaStoreException e) {
      return -1L;
    }
  }
  
  private void startFromLastSeqNum(long lastSn) throws IOException {
    LOG.info("Skipped fetching Alluxio Name Space, start applying alluxio journal entry from " + lastSn);
    submitEntryFetchAndApplyTask(lastSn);
    try {
      finishedCallback.call();
    } catch (Exception e) {
      LOG.error("Call back failed", e);
    }    
  }

  private void submitEntryFetchAndApplyTask(long lastSn) throws IOException {
    entryFetchAndApplyFuture =
        scheduledExecutorService.scheduleAtFixedRate(
            new AlluxioEntryFetchAndApplyTask(conf, metaStore, alluxioEntryApplier, lastSn),
            0,
            100,
            TimeUnit.MILLISECONDS);
  }

  private void startWithFetchingAlluxioNameSpace() throws IOException {
    ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(scheduledExecutorService);
    entryInotifyFile = new File("/tmp/entry-inotify-" + new Random().nextLong());
    entryQueueFile = new QueueFile(entryInotifyFile);
    long startSn = AlluxioJournalUtil.getCurrentSeqNum(conf);
    LOG.info("Start fetching alluxio namespace with current journal entry sequence number = " + startSn);
    alluxioNamespaceFetcher.startFetch();
    entryFetchFuture = scheduledExecutorService.scheduleAtFixedRate(
        new AlluxioEntryFetchTask(entryQueueFile, conf, startSn), 0, 100, TimeUnit.MILLISECONDS);
    entryApplyTask = new AlluxioEntryApplyTask(alluxioNamespaceFetcher, alluxioEntryApplier, entryQueueFile, conf, startSn);
    ListenableFuture<?> future = listeningExecutorService.submit(entryApplyTask);
    Futures.addCallback(future, new AlluxioNameSpaceFetcherCallBack(), scheduledExecutorService);
    LOG.info("Start apply alluxio entry.");
  }

  private class AlluxioNameSpaceFetcherCallBack implements FutureCallback<Object> {

    @Override
    public void onSuccess(@Nullable Object o) {
      entryFetchFuture.cancel(false);
      alluxioNamespaceFetcher.stop();
      try {
        entryQueueFile.close();
        submitEntryFetchAndApplyTask(entryApplyTask.getLastSn());
        LOG.info("Alluxio Namespace fetch finished.");
        finishedCallback.call();
      } catch (Exception e) {
        LOG.error("Call back failed", e);
      }
    }

    @Override
    public void onFailure(Throwable throwable) {
      LOG.error("Alluxio NameSpace fetch failed", throwable);
    }
  }
  
  public void stop() {
    if (entryInotifyFile != null) {
      entryInotifyFile.delete();
    }
    if (entryFetchFuture != null) {
      entryFetchFuture.cancel(false);
    }
    if (entryFetchAndApplyFuture != null) {
      entryFetchAndApplyFuture.cancel(false);
    }
  }
  
  private static class AlluxioEntryFetchTask implements Runnable {
    private final QueueFile queueFile;
    private JournalReader journalReader;
    
    public AlluxioEntryFetchTask(QueueFile queueFile, SmartConf conf, long startSn) {
      this.queueFile = queueFile;
      this.journalReader = AlluxioJournalUtil.getJournalReaderFromSn(conf, startSn);
    }

    @Override
    public void run() {
      try {
        JournalEntry journalEntry = journalReader.read();
        while (journalEntry != null) {
          byte[] seqEntry = journalEntry.toByteArray();
          this.queueFile.add(seqEntry);
          journalEntry = journalReader.read();
        }
      } catch (IOException | InvalidJournalEntryException e) {
        LOG.error("Alluxio entry enqueue error", e);
      }
    }
  }
  
  private static class AlluxioEntryApplyTask implements Runnable {
    private final AlluxioNamespaceFetcher namespaceFetcher;
    private final AlluxioEntryApplier entryApplier;
    private final QueueFile queueFile;
    private long lastSn;
    private SmartConf conf;
    private List<String> ignoreList;
    
    public AlluxioEntryApplyTask(AlluxioNamespaceFetcher namespaceFetcher, AlluxioEntryApplier entryApplier,
                                 QueueFile queueFile, SmartConf conf, long lastSn) {
      this.namespaceFetcher = namespaceFetcher;
      this.entryApplier = entryApplier;
      this.queueFile = queueFile;
      this.conf = conf;
      this.lastSn = lastSn;
      this.ignoreList = getIgnoreDirFromConfig();
    }

    public List<String> getIgnoreDirFromConfig() {
      String ignoreDirs = this.conf.get(SmartConfKeys.SMART_IGNORE_DIRS_KEY);
      List<String> ignoreList;
      if (ignoreDirs == null || ignoreDirs.equals("")) {
        ignoreList = new ArrayList<>();
      } else {
        ignoreList = Arrays.asList(ignoreDirs.split(","));
      }
      for (int i = 0; i < ignoreList.size(); i++) {
        if (!ignoreList.get(i).endsWith("/")) {
          ignoreList.set(i, ignoreList.get(i).concat("/"));
        }
      }
      return ignoreList;
    }

    public boolean fetchPathInIgnoreList(String path) {
      if (!path.endsWith("/")) {
        path = path.concat("/");
      }
      for (int i = 0; i < ignoreList.size(); i++) {
        if (path.equals(ignoreList.get(i))) {
          return true;
        }
      }
      return false;
    }
    
    public boolean ignoreEntry(JournalEntry entry) throws MetaStoreException {
      String inodePath;
      if (entry.hasInodeDirectory()) {
        inodePath = entryApplier.getPathFromInodeDir(entry.getInodeDirectory());
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasInodeFile()) {
        inodePath = entryApplier.getPathFromInodeFile(entry.getInodeFile());
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasInodeLastModificationTime()) {
        inodePath = entryApplier.getPathByFileId(entry.getInodeLastModificationTime().getId());
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasPersistDirectory()) {
        inodePath = entryApplier.getPathByFileId(entry.getPersistDirectory().getId());
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasSetAttribute()) {
        inodePath = entryApplier.getPathByFileId(entry.getSetAttribute().getId());
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasRename()) {
        inodePath =  entryApplier.getPathByFileId(entry.getRename().getId());
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasDeleteFile()) {
        inodePath = entryApplier.getPathByFileId(entry.getDeleteFile().getId());
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasAddMountPoint()) {
        inodePath = entry.getAddMountPoint().getAlluxioPath();
        return fetchPathInIgnoreList(inodePath);
      } else if (entry.hasDeleteMountPoint()) {
        inodePath = entry.getDeleteMountPoint().getAlluxioPath();
        return fetchPathInIgnoreList(inodePath);
      } 
      return true;
    }

    @Override
    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          if (!namespaceFetcher.fetchFinished()) {
            Thread.sleep(100);
          } else {
            while (!queueFile.isEmpty()) {
              JournalEntry entry = JournalEntry.parseFrom(queueFile.peek());
              queueFile.remove();
              if (!ignoreEntry(entry)) {
                this.entryApplier.apply(entry);
                this.lastSn = entry.getSequenceNumber();
              }
            }
            break;
          }
        }
      } catch (InterruptedException | IOException | MetaStoreException e) {
        LOG.error("Alluxio entry dequeue error", e);
      }
    }
    
    public long getLastSn() {
      return this.lastSn;
    }
  }
}
