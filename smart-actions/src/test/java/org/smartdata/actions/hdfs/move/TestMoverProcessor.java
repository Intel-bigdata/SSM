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
package org.smartdata.actions.hdfs.move;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.junit.Test;
import org.smartdata.actions.hdfs.ActionMiniCluster;
import org.smartdata.model.actions.hdfs.StorageMap;

import java.net.URI;

public class TestMoverProcessor extends ActionMiniCluster {

  @Test
  public void testGenPlan() throws Exception {
    Configuration conf = smartContext.getConf();
    URI namenode = cluster.getURI();

    String file = "/testfile";
    int nblocks = 4;
    DFSTestUtil.createFile(dfs, new Path(file), nblocks * DEFAULT_BLOCK_SIZE, (short) 2, 100);
    dfs.setStoragePolicy(new Path(file), "ALL_SSD");

    GetMoverSchedulerInfo schedulerInfo = new GetMoverSchedulerInfo(dfsClient);
    schedulerInfo.run();

    MoverStatus status = new MoverStatus();
    StorageMap storageMap = schedulerInfo.getStorages();
    MoverProcessor processor = new MoverProcessor(dfsClient, storageMap, status);
    ExitStatus exitStatus = processor.processNamespace(new Path(file));
    exitStatus.getExitCode();
  }
}
