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
package org.apache.hadoop.ssm.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Move to Cache Action
 */
public class MoveToCache extends ActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(MoveToCache.class);

    private DFSClient dfsClient;
    private String fileName;
    private Configuration conf;
    private LinkedBlockingQueue<String> actionEvents;
    private final String SSMPOOL = "SSMPool";

    public MoveToCache(DFSClient client, Configuration conf) {
        super(client);
        this.dfsClient = client;
        this.conf = conf;
        this.actionType = ActionType.CacheFile;
        this.actionEvents = new LinkedBlockingQueue<String>();
    }

    public ActionBase initial(String[] args){
        fileName = args[0];
        return this;
    }

    /**
     * Execute an action.
     * @return null.
     */
    public UUID execute(){
//        Action action = Action.getActionType("cache");
        //MoverExecutor.getInstance(dfsClient,conf).addActionEvent(fileName,action);
        addActionEvent(fileName);
        //MoverExecutor.getInstance(dfsClient,conf).run();
        runCache(fileName);
        return null;
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
        LOG.info("Action starts at " + new Date(System.currentTimeMillis()) +
            " : " + fileName + " -> " + "cache");
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