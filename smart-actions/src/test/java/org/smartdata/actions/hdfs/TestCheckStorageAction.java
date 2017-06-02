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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Test for CheckStorageAction.
 */
public class TestCheckStorageAction {
  private static final int DEFAULT_BLOCK_SIZE = 50;
  private Configuration conf;
  MiniDFSCluster cluster;
  DFSClient dfsClient;

  @Before
  public void init() throws Exception {
    conf = new HdfsConfiguration();
    initConf(conf);
    cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(5)
            .build();
    cluster.waitActive();
    dfsClient = cluster.getFileSystem().getClient();
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
  }

  @Test
  public void testCheckStorageAction() throws IOException {
    try {
      CheckStorageAction checkStorageAction = new CheckStorageAction();
      checkStorageAction.setDfsClient(dfsClient);
      final String file = "/testParallelMovers/file1";
      dfsClient.mkdirs("/testParallelMovers");

      // write to HDFS
      final OutputStream out = dfsClient.create(file, true);
      byte[] content = ("This is a file containing two blocks" +
          "......................").getBytes();
      out.write(content);
      out.close();

      // do CheckStorageAction
      Assert.assertEquals("CheckStorageAction", checkStorageAction.getName());
      checkStorageAction.init(new String[] {file});
      checkStorageAction.run();
      ActionStatus actionStatus = checkStorageAction.getActionStatus();
      System.out.println(StringUtils.formatTime(actionStatus.getRunningTime()));
      Assert.assertTrue(actionStatus.isFinished());
      Assert.assertTrue(actionStatus.isSuccessful());
      Assert.assertEquals(1.0f, actionStatus.getPercentage(), 0.00001f);

      ByteArrayOutputStream resultStream = actionStatus.getResultStream();
      System.out.println(resultStream);
    } finally {
      cluster.shutdown();
    }
  }
}
