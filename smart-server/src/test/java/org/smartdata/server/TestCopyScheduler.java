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
package org.smartdata.server;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.ActionInfo;
import org.smartdata.server.engine.CmdletManager;

import java.util.List;

public class TestCopyScheduler extends MiniSmartClusterHarness {

  @Test
  public void testSyncScheduler() throws Exception {
    waitTillSSMExitSafeMode();
    MetaStore metaStore = ssm.getMetaStore();
    metaStore.deleteAllFileDiff();
    metaStore.deleteAllFileInfo();
    metaStore.deleteAllCmdlets();
    metaStore.deleteAllActions();
    DistributedFileSystem dfs = cluster.getFileSystem();
    final String srcPath = "/src/";
    final String destPath = "/dest/";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // Write to src
    for (int i = 0; i < 3; i++) {
      // Create test files
      DFSTestUtil.createFile(dfs, new Path(srcPath + i), 1024, (short) 1, 1);
    }
    Thread.sleep(1000);
    CmdletManager cmdletManager = ssm.getCmdletManager();
    // Submit sync action
    for (int i = 0; i < 3; i++) {
      // Create test files
      cmdletManager.submitCmdlet("sync -file /src/" + i + " -dest /dest/" + i);
    }
    List<ActionInfo> actionInfos = cmdletManager.listNewCreatedActions("sync", 0);
    Assert.assertTrue(actionInfos.size() == 3);
    Thread.sleep(3000);
    for (int i = 0; i < 3; i++) {
      // Write 10 files
      Assert.assertTrue(dfs.exists(new Path(destPath + i)));
      System.out.printf("File %d is copied.\n", i);
    }
  }
}
