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


import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.server.metastore.MetaUtil;
import org.smartdata.server.metastore.TestDBUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestSmartServerCli {

  @Test
  public void testConfNameNodeRPCAddr() throws Exception {
    SmartConf config = new SmartConf();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(3).build();

    try {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(config);
      List<URI> uriList = new ArrayList<>(namenodes);

      SmartConf conf = new SmartConf();
      // Set db used
      String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
      String dbUrl = MetaUtil.SQLITE_URL_PREFIX + dbFile;
      conf.set(SmartConfKeys.DFS_SSM_DB_URL_KEY, dbUrl);

      // rpcServer start in SmartServer
      try {
        SmartServer.launchWith(conf);
        Assert.fail("Should not work without specifying "
            + SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY);
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains(
            SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY));
      }

      conf.set(SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY,
          uriList.get(0).toString());
      String[] args = new String[]{
          "-D",
          SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY + "="
              + uriList.get(0).toString()
      };

      SmartServer regServer = SmartServer.launchWith(args, conf);
      Thread.sleep(1000);
      regServer.shutdown();

      args = new String[] {
          "-h"
      };
      SmartServer.launchWith(args, conf);
    } finally {
      cluster.shutdown();
    }
  }
}
