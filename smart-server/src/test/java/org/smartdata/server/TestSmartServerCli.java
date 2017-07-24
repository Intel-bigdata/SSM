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
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.metastore.utils.TestDBUtil;

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
      String dbUrl = MetaStoreUtils.SQLITE_URL_PREFIX + dbFile;
      conf.set(SmartConfKeys.SMART_METASTORE_DB_URL_KEY, dbUrl);

      conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
          uriList.get(0).toString());
      String[] args = new String[]{
          "-D",
          SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY + "="
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
