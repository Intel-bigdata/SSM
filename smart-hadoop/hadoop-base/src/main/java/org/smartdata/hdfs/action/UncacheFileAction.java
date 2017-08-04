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
package org.smartdata.hdfs.action;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.smartdata.actions.Utils;
import org.smartdata.actions.annotation.ActionSignature;

import java.util.Map;

/**
 * An action to un-cache a file.
 */
@ActionSignature(
  actionId = "uncache",
  displayName = "uncache",
  usage = HdfsAction.FILE_PATH + " $file "
)
public class UncacheFileAction extends HdfsAction {
  private String fileName;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    fileName = args.get(FILE_PATH);
  }

  @Override
  protected void execute() throws Exception {
    if (fileName == null) {
      throw new IllegalArgumentException("File parameter is missing! ");
    }
    this.appendLog(
        String.format(
            "Action starts at %s : %s -> uncache", Utils.getFormatedCurrentTime(), fileName));
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
      this.appendLog(String.format("File %s is already cached. No action taken.", fileName));
      return;
    }
    dfsClient.removeCacheDirective(id);
  }
}
