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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionException;
import org.smartdata.actions.ActionStatus;

import java.util.Date;
import java.util.Map;

/**
 * An action to un-cache a file.
 */
public class UncacheFileAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(UncacheFileAction.class);

  private String fileName;

  public UncacheFileAction() {
    createStatus();
  }

  @Override
  protected void createStatus() {
    this.actionStatus = new CacheStatus();
    resultOut = actionStatus.getResultPrintStream();
    logOut = actionStatus.getLogPrintStream();
  }

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    fileName = args.get(FILE_PATH);
  }

  @Override
  protected void execute() throws ActionException {
    ActionStatus actionStatus = getActionStatus();
    actionStatus.begin();
    try {
      executeUncacheAction();
    } catch (Exception e) {
      throw new ActionException(e);
    }
  }

  private void executeUncacheAction() throws Exception {
    LOG.info("Action starts at {} : {} -> uncache",
        new Date(System.currentTimeMillis()), fileName);
    removeDirective(fileName);
  }

  @VisibleForTesting
  Long getCacheId(String fileName) throws Exception {
    CacheDirectiveInfo.Builder filterBuilder = new CacheDirectiveInfo.Builder();
    filterBuilder.setPath(new Path(fileName));
    CacheDirectiveInfo filter = filterBuilder.build();
    RemoteIterator<CacheDirectiveEntry> directiveEntries = dfsClient.listCacheDirectives(filter);
    if (!directiveEntries.hasNext()) {
      return null;
    }
    return directiveEntries.next().getInfo().getId();
  }

  private void removeDirective(String fileName) throws Exception {
    Long id = getCacheId(fileName);
    if (id == null) {
      LOG.info("File {} is already cached. No action taken.", fileName);
      return;
    }
    dfsClient.removeCacheDirective(id);
  }
}
