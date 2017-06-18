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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Move to Cache Unit Test
 */
public class TestCacheFile extends ActionMiniCluster {
  @Test
  public void testCacheFile() throws IOException {
    final String file = "/testCache/file";
    Path dir = new Path("/testCache");
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out = dfs.create(new Path(file));
    out.writeChars("testCache");
    out.close();

    CacheFileAction cacheAction = new CacheFileAction();
    cacheAction.setContext(smartContext);
    cacheAction.setDfsClient(dfsClient);
    Map<String, String> args = new HashMap();
    args.put(CacheFileAction.FILE_PATH, file);
    cacheAction.init(args);
    ActionStatus actionStatus = cacheAction.getActionStatus();
    try {
      Assert.assertEquals(false, cacheAction.isCached(file));
      cacheAction.run();
      Assert.assertEquals(true, cacheAction.isCached(file));
      Assert.assertTrue(actionStatus.isFinished());
      Assert.assertTrue(actionStatus.isSuccessful());
      System.out.println("Cache action running time : " +
          StringUtils.formatTime(actionStatus.getRunningTime()));
      Assert.assertEquals(1.0f, actionStatus.getPercentage(), 0.00001f);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
