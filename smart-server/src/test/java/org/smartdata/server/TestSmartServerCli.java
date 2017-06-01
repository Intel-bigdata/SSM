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
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.Util;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;

public class TestSmartServerCli {

  @Test
  public void testConfNameNodeRPCAddr() throws Exception {
    SmartConf conf = new SmartConf();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();

    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    List<URI> uriList = new ArrayList<>(namenodes);
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());

    // Set db used
    String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    String dbUrl = Util.SQLITE_URL_PREFIX + dbFile;
    conf.set(SmartConfKeys.DFS_SSM_DEFAULT_DB_URL_KEY, dbUrl);

    // rpcServer start in SmartServer
    try {
      SmartServer.createSSM(null, conf);
      Assert.fail("Should not work without specifying "
          + SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(
          SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY));
    }


    conf.set(SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY,
        uriList.get(0).toString());
    String[] args = new String[] {
        "-D",
        SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY + "="
            + uriList.get(0).toString()
    };

    SmartServer s = SmartServer.createSSM(args, conf);
    s.shutdown();

    String[] argsHelp = new String[] {
        "-h"
    };

    s = SmartServer.createSSM(argsHelp, conf);
    Assert.assertTrue(s == null);
    cluster.shutdown();
  }
}
