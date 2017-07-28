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
 * Test for OneSsdFileAction.
 */
public class TestOneSsdFileAction extends ActionMiniCluster {
  @Test
  public void testOneSsd() throws Exception {
    final String file = "/testOneSsd/file";
    Path dir = new Path("/testOneSsd");
    dfs.mkdirs(dir);
    // write to DISK
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out = dfs.create(new Path(file));
    out.writeChars("testOneSsd");
    out.close();

    // schedule move to Archive
    OneSsdFileAction action = new OneSsdFileAction();
    action.setDfsClient(dfsClient);
    action.setContext(smartContext);
    action.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap();
    args.put(OneSsdFileAction.FILE_PATH, file);
    action.init(args);
    action.run();

    // verify after movement
    LocatedBlock lb = dfsClient.getLocatedBlocks(file, 0).get(0);
    StorageType[] storageTypes = lb.getStorageTypes();
    int ssdCount = 0;
    int hddCount = 0;
    for (StorageType storageType : storageTypes) {
      if (storageType == StorageType.SSD) {
        ssdCount ++;
      }
      else if (storageType == StorageType.DISK) {
        hddCount ++;
      }
    }
    Assert.assertEquals(1, ssdCount);
    Assert.assertEquals(2, hddCount);
  }
}
