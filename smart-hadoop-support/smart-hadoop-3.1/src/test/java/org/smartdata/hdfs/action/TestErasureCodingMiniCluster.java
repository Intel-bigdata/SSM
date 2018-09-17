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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.junit.After;
import org.junit.Before;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.MiniClusterFactory;
import org.smartdata.hdfs.MiniClusterHarness;
import org.smartdata.hdfs.MiniClusterWithStoragesHarness;

import java.io.IOException;

public class TestErasureCodingMiniCluster {
  protected ErasureCodingPolicy ecPolicy;
  // use the default one, not the one in MiniClusterHarness
  public static final long BLOCK_SIZE = DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem dfs;
  protected DFSClient dfsClient;
  protected SmartContext smartContext;

  @Before
  public void init() throws Exception {
    SmartConf conf = new SmartConf();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    // use ErasureCodeConstants.XOR_2_1_SCHEMA
    ecPolicy = SystemErasureCodingPolicies.getPolicies().get(3);
    cluster = MiniClusterFactory.get().
        createWithStorages(ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits(), conf);
    // Add namenode URL to smartContext
    conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
        "hdfs://" + cluster.getNameNode().getNameNodeAddressHostPortString());
    smartContext = new SmartContext(conf);
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = dfs.getClient();
    dfsClient.enableErasureCodingPolicy(ecPolicy.getName());
  }

  public void createTestFile(String srcPath, long length) throws IOException {
//    DFSTestUtil.createFile(dfs, new Path(srcPath), length, (short) 3, 0L);
    DFSTestUtil.createFile(dfs, new Path(srcPath), 1024, length, BLOCK_SIZE, (short) 3, 0L);
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
