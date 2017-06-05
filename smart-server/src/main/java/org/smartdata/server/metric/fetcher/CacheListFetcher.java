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
import java.util.List;

public class CacheListFetcher {

  public CacheListFetcher() {
    this.fetchTask = new
  }



  private FetchTask fetchTask;

  private static class FetchTask implements Runnable {
    private DFSClient dfsClient;
    private DBAdapter dbAdapter;

    public FetchTask(DFSClient dfsClient, DBAdapter dbAdapter) {
      this.dfsClient = dfsClient;
      this.dbAdapter = dbAdapter;
    }

    @Override
    public void run() {
      try {
        CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
        filterBuilder.setPool("SSMPool");
        CacheDirectiveInfo filter = filterBuilder.build();
        RemoteIterator<CacheDirectiveEntry> cacheDirectives = dfsClient.listCacheDirectives(filter);
        if (cacheDirectives.hasNext()) {
          // TODO clear all cache file list
          // dbAdapter.clear
        }
        List<CachedFileStatus> cachedFileStatuses = new ArrayList<>();
        while(cacheDirectives.hasNext()) {
          CacheDirectiveInfo currentInfo = cacheDirectives.next().getInfo();
          cachedFileStatuses.add(new CachedFileStatus(currentInfo.getId(),
              currentInfo.getPath().getName(), Time.now(), Time.now(), 0));
        }
        dbAdapter.insertCachedFiles(cachedFileStatuses);
      } catch (IOException e) {
        e.printStackTrace();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
