package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.tools.CacheAdmin;
import org.apache.hadoop.util.ToolRunner;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.hadoop.hdfs.protocol.FilesInfo.STORAGEPOLICY;

/**
 * Created by root on 11/10/16.
 */
public class MoverExecutor {
  private static MoverExecutor instance;

  public static final byte MEMORY_STORAGE_POLICY_ID = 15;
  public static final String MEMORY_STORAGE_POLICY_NAME = "LAZY_PERSIST";
  public static final byte ALLSSD_STORAGE_POLICY_ID = 12;
  public static final String ALLSSD_STORAGE_POLICY_NAME = "ALL_SSD";
  public static final byte ONESSD_STORAGE_POLICY_ID = 10;
  public static final String ONESSD_STORAGE_POLICY_NAME = "ONE_SSD";
  public static final byte HOT_STORAGE_POLICY_ID = 7;
  public static final String HOT_STORAGE_POLICY_NAME = "HOT";
  public static final byte WARM_STORAGE_POLICY_ID = 5;
  public static final String WARM_STORAGE_POLICY_NAME = "WARM";
  public static final byte COLD_STORAGE_POLICY_ID = 2;
  public static final String COLD_STORAGE_POLICY_NAME = "COLD";

  private DFSClient dfsClient;
  Configuration conf;
  private LinkedBlockingQueue<FileAction> actionEvents;

  private final String SSMPOOL = "SSMPool";

  class FileAction {
    String fileName;
    Action action;

    FileAction(String fileName, Action action) {
      this.fileName = fileName;
      this.action = action;
    }
  }

  private MoverExecutor(DFSClient dfsClient, Configuration conf) {
    this.dfsClient = dfsClient;
    this.conf = conf;
    this.actionEvents = new LinkedBlockingQueue<>();
  }

  public static synchronized MoverExecutor getInstance(DFSClient dfsClient, Configuration conf) {
    if (instance == null) {
      instance = new MoverExecutor(dfsClient, conf);
    }
    return instance;
  }

  public static synchronized MoverExecutor getInstance() {
    return instance;
  }

  public void addActionEvent(String fileName, Action action) {
    try {
      actionEvents.put(new FileAction(fileName, action));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public FileAction getActionEvent() {
    try {
      return actionEvents.take();
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  public void run() {
    ExecutorService exec = Executors.newCachedThreadPool();
    exec.execute(new ExecutorRunner());
  }

  class ExecutorRunner implements Runnable {

    public void run() {
      while (true) {
        FileAction fileAction = instance.getActionEvent();
        String fileName = fileAction.fileName;
        Action action = fileAction.action;
        switch (action) {
          case SSD:
            runSSD(fileName);
            break;
          case ARCHIVE:
            runArchive(fileName);
            break;
          case CACHE:
            runCache(fileName);
            break;
          default:
        }
      }
    }

    private void runCache(String fileName) {
      createPool();
      if (isCached(fileName)) {
        return;
      }
      System.out.println("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "cache");
      addDirective(fileName);
    }

    private void createPool() {
      try {
        RemoteIterator<CachePoolEntry> poolEntries = dfsClient.listCachePools();
        while (poolEntries.hasNext()) {
          CachePoolEntry poolEntry = poolEntries.next();
          if (poolEntry.getInfo().getPoolName().equals(SSMPOOL)) {
            return;
          }
        }
        dfsClient.addCachePool(new CachePoolInfo(SSMPOOL));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private boolean isCached(String fileName) {
      CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
      filterBuilder.setPath(new Path(fileName));
      CacheDirectiveInfo filter = filterBuilder.build();
      try {
        RemoteIterator<CacheDirectiveEntry> directiveEntries = dfsClient.listCacheDirectives(filter);
        return directiveEntries.hasNext();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void addDirective(String fileName) {
      CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
      filterBuilder.setPath(new Path(fileName));
      filterBuilder.setPool(SSMPOOL);
      CacheDirectiveInfo filter = filterBuilder.build();
      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      try {
        dfsClient.addCacheDirective(filter, flags);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void runArchive(String fileName) {
      if (getStoragePolicy(fileName) == COLD_STORAGE_POLICY_ID) {
        return;
      }
      System.out.println("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "archive");
      try {
        dfsClient.setStoragePolicy(fileName, "COLD");
      } catch (Exception e) {
        return;
      }
      try {
        ToolRunner.run(conf, new Mover.Cli(),
                new String[]{"-p", fileName});
      } catch (Exception e) {
        return;
      }
    }

    private void runSSD(String fileName) {
      if (getStoragePolicy(fileName) == ALLSSD_STORAGE_POLICY_ID) {
        return;
      }
      System.out.println("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "ssd");
      try {
        dfsClient.setStoragePolicy(fileName, "ALL_SSD");
      } catch (Exception e) {
        return;
      }
      try {
        ToolRunner.run(conf, new Mover.Cli(),
                new String[]{"-p", fileName});
      } catch (Exception e) {
        return;
      }
    }
  }

  private byte getStoragePolicy(String fileName) {
    String[] paths = {fileName};
    FilesInfo filesInfo;
    try {
       filesInfo = dfsClient.getFilesInfo(paths, STORAGEPOLICY, false, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    byte storagePolicy = filesInfo.getStoragePolicy().get(0);
    return storagePolicy;
  }

}
