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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.MockActionStatusReporter;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for UncacheFileAction.
 */
public class TestUncacheFileAction extends ActionMiniCluster {
  @Test
  public void testUncacheFile() throws Exception {
    final String file = "/testUncache/file";
    Path dir = new Path("/testUncache");
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out = dfs.create(new Path(file));
    out.writeChars("testUncache");
    out.close();

    CacheFileAction cacheFileAction = new CacheFileAction();
    cacheFileAction.setDfsClient(dfsClient);
    cacheFileAction.setContext(smartContext);
    cacheFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> argsCache = new HashMap();
    argsCache.put(CacheFileAction.FILE_PATH, file);
    cacheFileAction.init(argsCache);

    UncacheFileAction uncacheFileAction = new UncacheFileAction();
    uncacheFileAction.setDfsClient(dfsClient);
    uncacheFileAction.setContext(smartContext);
    uncacheFileAction.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> argsUncache = new HashMap();
    argsUncache.put(UncacheFileAction.FILE_PATH, file);
    uncacheFileAction.init(argsUncache);

    cacheFileAction.run();
    Assert.assertTrue(cacheFileAction.isCached(file));

    uncacheFileAction.run();
    Assert.assertFalse(cacheFileAction.isCached(file));
  }

  @Test
  public void testUncacheNoncachedFile() throws Exception {
    final String file = "/testUncache/file";
    Path dir = new Path("/testUncache");
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out = dfs.create(new Path(file));
    out.writeChars("testUncache");
    out.close();

    UncacheFileAction uncacheFileAction = new UncacheFileAction();
    uncacheFileAction.setDfsClient(dfsClient);
    uncacheFileAction.setContext(smartContext);
    uncacheFileAction.setStatusReporter(new MockActionStatusReporter());

    Map<String, String> argsUncache = new HashMap();
    argsUncache.put(UncacheFileAction.FILE_PATH, file);
    uncacheFileAction.init(argsUncache);

    uncacheFileAction.run();
    CacheFileAction cacheFileAction = new CacheFileAction();
    cacheFileAction.setStatusReporter(new MockActionStatusReporter());
    cacheFileAction.setDfsClient(dfsClient);
    cacheFileAction.setContext(smartContext);
    Assert.assertFalse(cacheFileAction.isCached(file));
  }
}
