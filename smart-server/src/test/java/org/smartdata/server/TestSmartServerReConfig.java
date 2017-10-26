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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.SmartServiceState;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.TestDBUtil;
import org.smartdata.metastore.utils.MetaStoreUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;

public class TestSmartServerReConfig {
  protected SmartConf conf;
  protected MiniDFSCluster cluster;
  protected SmartServer ssm;
  protected String dbFile;
  protected String dbUrl;

  private static final int DEFAULT_BLOCK_SIZE = 100;

  @Test
  public void setUp() throws Exception {
    try {
      conf = new SmartConf();
      initConf(conf);
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(3)
          .build();
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      List<URI> uriList = new ArrayList<>(namenodes);
      conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());
      conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
          uriList.get(0).toString());

      // Set db used
      dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
      dbUrl = MetaStoreUtils.SQLITE_URL_PREFIX + dbFile;
      conf.set(SmartConfKeys.SMART_METASTORE_DB_URL_KEY, dbUrl);

      SmartConf serverConf = new SmartConf();
      serverConf.set(SmartConfKeys.SMART_DFS_ENABLED, "false");
      serverConf.set(SmartConfKeys.SMART_METASTORE_DB_URL_KEY, dbUrl);
      // rpcServer start in SmartServer
      ssm = SmartServer.launchWith(serverConf);

      Thread.sleep(2000);
      Assert.assertTrue(ssm.getSSMServiceState() == SmartServiceState.DISABLED);

      serverConf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
          uriList.get(0).toString());
      ssm.enable();

      Thread.sleep(2000);

      Assert.assertTrue(ssm.getSSMServiceState() == SmartServiceState.ACTIVE);

    } finally {
      if (ssm != null) {
        ssm.shutdown();
      }

      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }
}
