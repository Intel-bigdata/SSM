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

package org.smartdata.actions.hdfs;

import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionStatus;
import org.smartdata.actions.ActionType;

import java.util.Date;
import java.util.EnumSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Move to Cache Action
 */
public class CacheFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(CacheFileAction.class);

  private String fileName;
  private LinkedBlockingQueue<String> actionEvents;
  private final String SSMPOOL = "SSMPool";
  private ActionType actionType;

  public CacheFileAction() {
    this.actionType = ActionType.CacheFile;
    this.actionEvents = new LinkedBlockingQueue<>();
    createActionStatus();
  }

  @Override
  protected void createActionStatus() {
    this.actionStatus = new CacheStatus();
    resultOut = actionStatus.getResultPrintStream();
    logOut = actionStatus.getLogPrintStream();
  }

  @Override
  public void init(String[] args) {
    super.init(args);
    fileName = args[0];
  }

  protected void execute() {
    ActionStatus actionStatus = getActionStatus();
    actionStatus.setStartTime();
    try {
      addActionEvent(fileName);
      executeCacheAction(fileName);
      actionStatus.setSuccessful(true);
    } catch (Exception e) {
      actionStatus.setSuccessful(false);
      throw new RuntimeException(e);
    } finally {
      actionStatus.setFinished(true);
    }
  }

  public void addActionEvent(String fileName) throws Exception {
    actionEvents.put(fileName);
  }

  private void executeCacheAction(String fileName) throws Exception {
    createCachePool();
    if (isCached(fileName)) {
      return;
    }
    LOG.info("Action starts at {} : {} -> cache",
        new Date(System.currentTimeMillis()), fileName);
    addDirective(fileName);
  }

  private void createCachePool() throws Exception {
    RemoteIterator<CachePoolEntry> poolEntries = dfsClient.listCachePools();
    while (poolEntries.hasNext()) {
      CachePoolEntry poolEntry = poolEntries.next();
      if (poolEntry.getInfo().getPoolName().equals(SSMPOOL)) {
        return;
      }
    }
    dfsClient.addCachePool(new CachePoolInfo(SSMPOOL));
  }

  public boolean isCached(String fileName) throws Exception {
    CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
    filterBuilder.setPath(new Path(fileName));
    CacheDirectiveInfo filter = filterBuilder.build();
    RemoteIterator<CacheDirectiveEntry> directiveEntries = dfsClient.listCacheDirectives(filter);
    return directiveEntries.hasNext();
  }

  private void addDirective(String fileName) throws Exception {
    CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
    filterBuilder.setPath(new Path(fileName));
    filterBuilder.setPool(SSMPOOL);
    CacheDirectiveInfo filter = filterBuilder.build();
    EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
    dfsClient.addCacheDirective(filter, flags);
  }
}
