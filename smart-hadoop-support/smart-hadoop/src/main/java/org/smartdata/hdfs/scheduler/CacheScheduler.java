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
package org.smartdata.hdfs.scheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.LaunchCmdlet;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CacheScheduler extends ActionSchedulerService {

  public static final String CACHE_ACTIION = "cache";
  public static final String UNCACHE_ACTIION = "uncache";
  public static final List<String> ACTIONS = Arrays.asList(CACHE_ACTIION, UNCACHE_ACTIION);
  public static final String SSM_POOL = "SSMPool";
  private Set<String> fileLock;
  private DFSClient dfsClient;
  private boolean isCachePoolCreated;

  public CacheScheduler(SmartContext context, MetaStore metaStore) {
    super(context, metaStore);
    fileLock = new HashSet<>();
    isCachePoolCreated = false;
  }

  @Override
  public List<String> getSupportedActions() {
    return ACTIONS;
  }

  public boolean isLocked(ActionInfo actionInfo) {
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    return fileLock.contains(srcPath);
  }

  @VisibleForTesting
  public Set<String> getFileLock() {
    return fileLock;
  }

  @Override
  public boolean onSubmit(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
    if (isLocked(actionInfo)) {
      return false;
    }
    return true;
  }

  @Override
  public ScheduleResult onSchedule(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      LaunchCmdlet cmdlet, LaunchAction action, int actionIndex) {
    String srcPath = actionInfo.getArgs().get(HdfsAction.FILE_PATH);
    fileLock.add(srcPath);
    return ScheduleResult.SUCCESS;
  }

  @Override
  public void init() throws IOException {
    URI nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
    dfsClient = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
    if (!isCachePoolCreated) {
      createCachePool();
      isCachePoolCreated = true;
    }
  }

  private void createCachePool() throws IOException {
    RemoteIterator<CachePoolEntry> poolEntries = dfsClient.listCachePools();
    while (poolEntries.hasNext()) {
      CachePoolEntry poolEntry = poolEntries.next();
      if (poolEntry.getInfo().getPoolName().equals(SSM_POOL)) {
        return;
      }
    }
    dfsClient.addCachePool(new CachePoolInfo(SSM_POOL));
  }

  @Override
  public void onActionFinished(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
    if (!ACTIONS.contains(actionInfo.getActionName())) {
      return;
    }
    if (isLocked(actionInfo)) {
      fileLock.remove(actionInfo.getArgs().get(HdfsAction.FILE_PATH));
    }
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }
}
