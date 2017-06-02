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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.actions.ActionStatus;
import org.smartdata.conf.SmartConf;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Test for SetStoragePolicyAction.
 */
public class TestSetStoragePolicyAction {
  private static final byte MEMORY_STORAGE_POLICY_ID = 15;
  private static final byte ALLSSD_STORAGE_POLICY_ID = 12;
  private static final byte ONESSD_STORAGE_POLICY_ID = 10;
  private static final byte HOT_STORAGE_POLICY_ID = 7;
  private static final byte WARM_STORAGE_POLICY_ID = 5;
  private static final byte COLD_STORAGE_POLICY_ID = 2;

  private static final int DEFAULT_BLOCK_SIZE = 50;
  MiniDFSCluster cluster;
  DistributedFileSystem dfs;
  DFSClient dfsClient;
  SmartContext smartContext;

  @Before
  public void init() throws Exception {
    SmartConf conf = new SmartConf();
    initConf(conf);
    cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(3)
            .storagesPerDatanode(4)
            .storageTypes(new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
                    StorageType.SSD, StorageType.RAM_DISK})
            .build();
    cluster.waitActive();
    dfsClient = cluster.getFileSystem().getClient();
    smartContext = new SmartContext(conf);
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
  }

  @Test
  public void testDifferentPolicies() throws IOException {
    try {
      final String file = "/testStoragePolicy/file";
      dfsClient.mkdirs("/testStoragePolicy");

      // write to HDFS
      final OutputStream out = dfsClient.create(file, true);
      byte[] content = "Hello".getBytes();
      out.write(content);
      out.close();

      byte policy = setStoragePolicy(file, "ALL_SSD");
      Assert.assertEquals(ALLSSD_STORAGE_POLICY_ID, policy);

      policy = setStoragePolicy(file, "COLD");
      Assert.assertEquals(COLD_STORAGE_POLICY_ID, policy);

      policy = setStoragePolicy(file, "ONE_SSD");
      Assert.assertEquals(ONESSD_STORAGE_POLICY_ID, policy);

      policy = setStoragePolicy(file, "HOT");
      Assert.assertEquals(HOT_STORAGE_POLICY_ID, policy);

      policy = setStoragePolicy(file, "WARM");
      Assert.assertEquals(WARM_STORAGE_POLICY_ID, policy);
    } finally {
      cluster.shutdown();
    }
  }

  private byte setStoragePolicy(String file, String storagePolicy)
      throws IOException {
    SetStoragePolicyAction action = new SetStoragePolicyAction();
    action.setDfsClient(dfsClient);
    action.setContext(smartContext);
    action.init(new String[] {file, storagePolicy});
    action.run();
    ActionStatus status = action.getActionStatus();

    // check results
    System.out.println("Action running time = " +
        StringUtils.formatTime(status.getRunningTime()));
    Assert.assertTrue(status.isFinished());
    Assert.assertTrue(status.isSuccessful());
    Assert.assertEquals(1.0f, status.getPercentage(), 0.00001f);
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(file);
    return fileStatus.getStoragePolicy();
  }
}
