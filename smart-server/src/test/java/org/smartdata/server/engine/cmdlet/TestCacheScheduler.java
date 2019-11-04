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
package org.smartdata.server.engine.cmdlet;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.junit.Test;
import org.smartdata.hdfs.scheduler.CacheScheduler;
import org.smartdata.model.CmdletState;
import org.smartdata.model.action.ActionScheduler;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.CmdletManager;

import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCacheScheduler extends MiniSmartClusterHarness {

  @Test(timeout = 180000)
  public void testCacheFile() throws Exception {
    waitTillSSMExitSafeMode();
    String filePath = new String("/testFile");
    FSDataOutputStream out = dfs.create(new Path(filePath));
    out.writeChars("test content");
    out.close();

    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cid = cmdletManager.submitCmdlet("cache -file " + filePath);
    while (true) {
      if (cmdletManager.getCmdletInfo(cid).getState().equals(CmdletState.DONE)) {
        break;
      }
    }
    RemoteIterator<CachePoolEntry> poolEntries = dfsClient.listCachePools();
    while (poolEntries.hasNext()) {
      CachePoolEntry poolEntry = poolEntries.next();
      if (poolEntry.getInfo().getPoolName().equals(CacheScheduler.SSM_POOL)) {
        return;
      }
      fail("A cache pool should be created by SSM: " + CacheScheduler.SSM_POOL);
    }
    // Currently, there is only one scheduler for cache action
    ActionScheduler actionScheduler = cmdletManager.getSchedulers("cache").get(0);
    assertTrue(actionScheduler instanceof CacheScheduler);
    Set<String> fileLock = ((CacheScheduler) actionScheduler).getFileLock();
    // There is no file locked after the action is finished.
    assertTrue(fileLock.isEmpty());
  }
}
