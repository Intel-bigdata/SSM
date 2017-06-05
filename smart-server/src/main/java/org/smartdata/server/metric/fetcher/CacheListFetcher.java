package org.smartdata.server.metric.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.smartdata.actions.hdfs.CacheFileAction;
import org.smartdata.actions.hdfs.CacheStatus;

import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.server.metastore.DBAdapter;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class CacheListFetcher {

  private static final Long DEFAULT_INTERVAL = 5 * 1000L;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Long fetchInterval;
  private FetchTask fetchTask;
  private ScheduledFuture scheduledFuture;


  public CacheListFetcher(
      Long fetchInterval,
      DFSClient dfsClient, DBAdapter dbAdapter,
      ScheduledExecutorService service) {
    this.fetchInterval = fetchInterval;
    this.fetchTask = new FetchTask(dfsClient, dbAdapter);
    this.scheduledExecutorService = service;
  }

  public CacheListFetcher(
      Long fetchInterval,
      DFSClient dfsClient, DBAdapter dbAdapter) {
    this(fetchInterval, dfsClient, dbAdapter, Executors.newSingleThreadScheduledExecutor());
  }

  public CacheListFetcher(
      DFSClient dfsClient, DBAdapter dbAdapter) {
    this(DEFAULT_INTERVAL, dfsClient, dbAdapter, Executors.newSingleThreadScheduledExecutor());
  }

  public CacheListFetcher(
      DFSClient dfsClient, DBAdapter dbAdapter,
      ScheduledExecutorService service) {
    this(DEFAULT_INTERVAL, dfsClient, dbAdapter, service);
  }

  public void start() {
    Long current = System.currentTimeMillis();
    Long toWait = fetchInterval - (current % fetchInterval);
    this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
        fetchTask, toWait, fetchInterval, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    if (scheduledFuture != null) {
      this.scheduledFuture.cancel(false);
    }
  }

  private static class FetchTask extends Thread {
    private DFSClient dfsClient;
    private DBAdapter dbAdapter;
    private Set<Long> fileSet;

    public FetchTask(DFSClient dfsClient, DBAdapter dbAdapter) {
      this.dfsClient = dfsClient;
      this.dbAdapter = dbAdapter;
      this.fileSet = new HashSet<>();
    }

    @Override
    public void run() {
      try {
        CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
        filterBuilder.setPool("SSMPool");
        CacheDirectiveInfo filter = filterBuilder.build();
        RemoteIterator<CacheDirectiveEntry> cacheDirectives = dfsClient.listCacheDirectives(filter);
        Set<Long> newFileSet = new HashSet<>();
        List<CachedFileStatus> cachedFileStatuses = new ArrayList<>();
        // Add new cache files to DB
        while(cacheDirectives.hasNext()) {
          CacheDirectiveInfo currentInfo = cacheDirectives.next().getInfo();
          Long fid = currentInfo.getId();
          newFileSet.add(fid);
          if (!fileSet.contains(fid)) {
            cachedFileStatuses.add(new CachedFileStatus(currentInfo.getId(),
                currentInfo.getPath().getName(), Time.now(), Time.now(), 0));
          }
        }
        dbAdapter.insertCachedFiles(cachedFileStatuses);
        // Remove uncached files from DB
        for (Long fid: fileSet) {
          if (!newFileSet.contains(fid)) {
            dbAdapter.deleteCachedFile(fid);
          }
        }
        fileSet = newFileSet;
      } catch (IOException e) {
        e.printStackTrace();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
