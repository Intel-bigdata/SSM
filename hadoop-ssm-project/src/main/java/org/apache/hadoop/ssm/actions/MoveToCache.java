package org.apache.hadoop.ssm.actions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.ssm.Action;

import java.util.EnumSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hadoop on 17-1-15.
 */
public class MoveToCache extends ActionBase {
    private static final Log LOG = LogFactory.getLog(MoveToCache.class);
    private static MoveToCache instance;
    private DFSClient dfsClient;
    private String fileName;
    private Configuration conf;
    private LinkedBlockingQueue<String> actionEvents;
    private final String SSMPOOL = "SSMPool";

    public MoveToCache(DFSClient client) {
        super(client);
        this.dfsClient=client;
        this.actionEvents = new LinkedBlockingQueue<String>();
    }

    public static MoveToCache getInstance(DFSClient dfsClient, Configuration conf) {
        if (instance == null) {
            instance = new MoveToCache(dfsClient);
            instance.conf=conf;
        }

        return instance;
    }

    public  void initial(String[] args){
        fileName=args[0];
    }

    /**
     * Execute an action.
     * @return true if success, otherwise return false.
     */
    public  boolean execute(){
        Action action =Action.getActionType("cache");

        //MoverExecutor.getInstance(dfsClient,conf).addActionEvent(fileName,action);
        MoveToCache.getInstance(dfsClient,conf).addActionEvent(fileName);
        //MoverExecutor.getInstance(dfsClient,conf).run();
        MoveToCache.getInstance(dfsClient,conf).runCache(fileName);
        return true;
    }

    public void addActionEvent(String fileName) {
        try {
            actionEvents.put(fileName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runCache(String fileName) {
        createPool();
        if (isCached(fileName)) {
            return;
        }
        LOG.info("*" + System.currentTimeMillis() + " : " + fileName + " -> " + "cache");
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

    public boolean isCached(String fileName) {
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


}