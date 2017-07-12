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
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.MockActionStatusReporter;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for AllSsdFileAction.
 */
public class TestAllSsdFileAction extends ActionMiniCluster {
  @Test
  public void testAllSsd() throws Exception {
    final String file = "/testAllSsd/file";
    Path dir = new Path("/testAllSsd");
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out = dfs.create(new Path(file));
    out.writeChars("testAllSSD");
    out.close();

    // schedule move to SSD
    AllSsdFileAction action = new AllSsdFileAction();
    action.setDfsClient(dfsClient);
    action.setContext(smartContext);
    action.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap();
    args.put(AllSsdFileAction.FILE_PATH, file);
    action.init(args);
    action.run();

    // verify after movement
    LocatedBlock lb = dfsClient.getLocatedBlocks(file, 0).get(0);
    StorageType[] storageTypes = lb.getStorageTypes();
    for (StorageType storageType : storageTypes) {
      Assert.assertTrue(StorageType.SSD == storageType);
    }
  }
}
