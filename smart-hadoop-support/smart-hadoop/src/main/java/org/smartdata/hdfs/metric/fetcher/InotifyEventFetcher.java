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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.squareup.tape.QueueFile;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartConstants;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.SystemInfo;
import org.smartdata.utils.StringUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class InotifyEventFetcher {
  private final DFSClient client;
  private final NamespaceFetcher nameSpaceFetcher;
  private final ScheduledExecutorService scheduledExecutorService;
  private final InotifyEventApplier applier;
  private final MetaStore metaStore;
  private Callable finishedCallback;
  private ScheduledFuture inotifyFetchFuture;
  private ScheduledFuture fetchAndApplyFuture;
  private EventApplyTask eventApplyTask;
  private java.io.File inotifyFile;
  private QueueFile queueFile;
  private SmartConf conf;
  public static final Logger LOG =
      LoggerFactory.getLogger(InotifyEventFetcher.class);
  public static List<String> oldList = new ArrayList<>();

  public InotifyEventFetcher(DFSClient client, MetaStore metaStore,
      ScheduledExecutorService service, Callable callBack) {
    this(client, metaStore, service, new InotifyEventApplier(metaStore, client), callBack, new SmartConf());
  }

  public InotifyEventFetcher(DFSClient client, MetaStore metaStore,
      ScheduledExecutorService service, Callable callBack, SmartConf conf) {
    this(client, metaStore, service, null, callBack, conf);
  }
  

  public InotifyEventFetcher(DFSClient client, MetaStore metaStore,
      ScheduledExecutorService service, InotifyEventApplier applier, Callable callBack) {
    this.client = client;
    this.applier = applier;
    this.metaStore = metaStore;
    this.scheduledExecutorService = service;
    this.finishedCallback = callBack;
    // use independent thread pool
    this.nameSpaceFetcher = new NamespaceFetcher(client, metaStore, null);
    this.conf = new SmartConf();
  }

  public InotifyEventFetcher(DFSClient client, MetaStore metaStore,
      ScheduledExecutorService service, InotifyEventApplier applier,
      Callable callBack, SmartConf conf) {
    this.client = client;
    this.metaStore = metaStore;
    this.scheduledExecutorService = service;
    this.finishedCallback = callBack;
    this.conf = conf;
    this.nameSpaceFetcher = new NamespaceFetcher(client, metaStore, null, conf);
    this.applier = new InotifyEventApplier(metaStore, client, nameSpaceFetcher);
  }

  public void start() throws IOException {
    boolean ignore = conf.getBoolean(
        SmartConfKeys.SMART_NAMESPACE_FETCHER_IGNORE_UNSUCCESSIVE_INOTIFY_EVENT_KEY,
        SmartConfKeys.SMART_NAMESPACE_FETCHER_IGNORE_UNSUCCESSIVE_INOTIFY_EVENT_DEFAULT);
    if (!ignore) {
      Long lastTxid = getLastTxid();
      //If whitelist is changed, the whole namespace will be fetched when servers restart
      if (lastTxid != null && lastTxid != -1 && canContinueFromLastTxid(client, lastTxid)
              && !isWhitelistChanged(conf, metaStore)) {
        startFromLastTxid(lastTxid);
      } else {
        startWithFetchingNameSpace();
        LOG.info("Start fetch namespace fully!");
      }
      //Update old whitelist
      try {
        String currentList = StringUtil.join(",", conf.getCoverDir());
        if (currentList.isEmpty()){
          currentList = "/";
        }
        metaStore.updateWhitelistTable(currentList);
      } catch (MetaStoreException e) {
        LOG.warn("Failed to update whitelist.", e);
      }
    } else {
      long id = client.getNamenode().getCurrentEditLogTxid();
      LOG.warn("NOTE: Incomplete iNotify event may cause unpredictable consequences. "
          + "This should only be used for testing.");
      startFromLastTxid(id);
    }
  }

  @VisibleForTesting
  static boolean canContinueFromLastTxid(DFSClient client, Long lastId) {
    try {
      if (client.getNamenode().getCurrentEditLogTxid() == lastId) {
        return true;
      }
      DFSInotifyEventInputStream is = client.getInotifyEventStream(lastId);
      EventBatch eventBatch = is.poll();
      return eventBatch != null;
    } catch (Exception e) {
      return false;
    }
  }

  private Long getLastTxid() {
    try {
      SystemInfo info =
          metaStore.getSystemInfoByProperty(SmartConstants.SMART_HDFS_LAST_INOTIFY_TXID);
      return info != null ? Long.parseLong(info.getValue()) : -1L;
    } catch (MetaStoreException e) {
      return -1L;
    }
  }

  private void startWithFetchingNameSpace() throws IOException {
    ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(scheduledExecutorService);
    inotifyFile = new File("/tmp/inotify" + new Random().nextLong());
    queueFile = new QueueFile(inotifyFile);
    long startId = client.getNamenode().getCurrentEditLogTxid();
    LOG.info("Start fetching namespace with current edit log txid = " + startId);
    nameSpaceFetcher.startFetch();
    inotifyFetchFuture = scheduledExecutorService.scheduleAtFixedRate(
      new InotifyFetchTask(queueFile, client, startId), 0, 100, TimeUnit.MILLISECONDS);
    eventApplyTask = new EventApplyTask(nameSpaceFetcher, applier, queueFile, startId, conf);
    ListenableFuture<?> future = listeningExecutorService.submit(eventApplyTask);
    Futures.addCallback(future, new NameSpaceFetcherCallBack(), scheduledExecutorService);
    LOG.info("Start apply iNotify events.");
  }

  private void startFromLastTxid(long lastId) throws IOException {
    LOG.info("Skipped fetching Name Space, start applying inotify events from " + lastId);
    submitFetchAndApplyTask(lastId);
    try {
      finishedCallback.call();
    } catch (Exception e) {
      LOG.error("Call back failed", e);
    }
  }

  private void submitFetchAndApplyTask(long lastId) throws IOException {
    fetchAndApplyFuture =
        scheduledExecutorService.scheduleAtFixedRate(
            new InotifyFetchAndApplyTask(client, metaStore, applier, lastId),
            0,
            100,
            TimeUnit.MILLISECONDS);
  }

  private class NameSpaceFetcherCallBack implements FutureCallback<Object> {

    @Override
    public void onSuccess(Object o) {
      inotifyFetchFuture.cancel(false);
      nameSpaceFetcher.stop();
      try {
        queueFile.close();
        submitFetchAndApplyTask(eventApplyTask.getLastId());
        LOG.info("Name space fetch finished.");
        finishedCallback.call();
      } catch (Exception e) {
        LOG.error("Call back failed", e);
      }
    }

    @Override
    public void onFailure(Throwable throwable) {
      LOG.error("NameSpaceFetcher failed", throwable);
    }
  }

  public void stop() {
    if (inotifyFile != null) {
      inotifyFile.delete();
    }
    if (inotifyFetchFuture != null) {
      inotifyFetchFuture.cancel(false);
    }
    if (fetchAndApplyFuture != null){
      fetchAndApplyFuture.cancel(false);
    }
  }

  /**
   *Verify if whitelist changed. It influences namespace fetching process.
   */
  public static boolean isWhitelistChanged(SmartConf conf, MetaStore metaStore) {
    List<String> currentList = conf.getCoverDir();
    try {
      oldList = metaStore.getLastFetchedDirs();
    } catch (MetaStoreException e) {
      LOG.warn("Failed to get last fetched dirs.", e);
    }
    if (currentList.size() != oldList.size()) {
      return true;
    }
    Set<String> set = new HashSet<>();
    for (String s : oldList) {
      set.add(s);
    }
    for (String s : currentList) {
      if (!set.contains(s)) {
        return true;
      }
    }
    return false;
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

  private static class EventApplyTask implements Runnable {
    private final NamespaceFetcher namespaceFetcher;
    private final InotifyEventApplier applier;
    private final QueueFile queueFile;
    private long lastId;
    private SmartConf conf;
    private List<String> ignoreList;
    private List<String> fetchList;

    public EventApplyTask(NamespaceFetcher namespaceFetcher, InotifyEventApplier applier,
        QueueFile queueFile, long lastId) {
      this.namespaceFetcher = namespaceFetcher;
      this.queueFile = queueFile;
      this.applier = applier;
      this.lastId = lastId;
      this.conf = new SmartConf();
      this.ignoreList = getIgnoreDirFromConfig();
    }

    public EventApplyTask(NamespaceFetcher namespaceFetcher, InotifyEventApplier applier,
        QueueFile queueFile, long lastId, SmartConf conf) {
      this.namespaceFetcher = namespaceFetcher;
      this.queueFile = queueFile;
      this.applier = applier;
      this.lastId = lastId;
      this.conf = conf;
      this.ignoreList = getIgnoreDirFromConfig();
      this.fetchList = getFetchDirFromConfig();
    }

    public List<String> getIgnoreDirFromConfig() {
      ignoreList = conf.getIgnoreDir();
      return ignoreList;
    }

    public List<String> getFetchDirFromConfig() {
      this.conf.getCoverDir();
      return fetchList;
    }

    public boolean shouldIgnore(String path) {
      if (!path.endsWith("/")) {
        path = path.concat("/");
      }
      for (String dir : ignoreList) {
        if (path.startsWith(dir)) {
          return true;
        }
      }
      if (fetchList.isEmpty()) {
        return false;
      }
      for (String dir : fetchList) {
        if (path.startsWith(dir)) {
          return false;
        }
      }
      return true;
    }

    public boolean ifEventIgnore(Event event) {
      String path;
      switch (event.getEventType()) {
        case CREATE:
          Event.CreateEvent createEvent = (Event.CreateEvent) event;
          path = createEvent.getPath();
          return shouldIgnore(path);
        case CLOSE:
          Event.CloseEvent closeEvent = (Event.CloseEvent) event;
          path = closeEvent.getPath();
          return shouldIgnore(path);
        case RENAME:
          Event.RenameEvent renameEvent = (Event.RenameEvent) event;
          path = renameEvent.getSrcPath();
          String dest = renameEvent.getDstPath();
          return shouldIgnore(path) && shouldIgnore(dest);
        case METADATA:
          Event.MetadataUpdateEvent metadataUpdateEvent = (Event.MetadataUpdateEvent) event;
          path = metadataUpdateEvent.getPath();
          return shouldIgnore(path);
        case APPEND:
          Event.AppendEvent appendEvent = (Event.AppendEvent) event;
          path = appendEvent.getPath();
          return shouldIgnore(path);
        case UNLINK:
          Event.UnlinkEvent unlinkEvent = (Event.UnlinkEvent) event;
          path = unlinkEvent.getPath();
          return shouldIgnore(path);
      }
      return true;
    }

    public Event[] deleteIgnoreEvent(Event[] events) {
      ArrayList<Event> eventArrayList = new ArrayList<>();
      for (int i = 0; i < events.length; i++) {
        if (!ifEventIgnore(events[i])) {
          eventArrayList.add(events[i]);
        }
      }
      return eventArrayList.toArray(new Event[eventArrayList.size()]);
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
              Event[] event = batch.getEvents();
              event = deleteIgnoreEvent(event);
              if (event.length > 0) {
                this.applier.apply(event);
                this.lastId = batch.getTxid();
              }
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
