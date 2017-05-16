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
package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ssm.protocol.SSMClient;
import org.apache.hadoop.ssm.sql.TestDBUtil;
import org.apache.hadoop.ssm.sql.Util;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSSMClient {

  @Test
  public void test() throws Exception {
    final Configuration conf = new SSMConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).build();
    // dfs not used , but datanode.ReplicaNotFoundException throws without dfs
    final DistributedFileSystem dfs = cluster.getFileSystem();

    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    List<URI> uriList = new ArrayList<>(namenodes);
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());
    conf.set(SSMConfigureKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY,
        uriList.get(0).toString());

    // Set db used
    String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    String dbUrl = Util.SQLITE_URL_PREFIX + dbFile;
    conf.set(SSMConfigureKeys.DFS_SSM_DEFAULT_DB_URL_KEY, dbUrl);

    // rpcServer start in SSMServer
    SSMServer.createSSM(null, conf);
    SSMClient ssmClient = new SSMClient(conf);

    while (true) {
      //test getServiceStatus
      String state = ssmClient.getServiceState().getName();
      if ("ACTIVE".equals(state)) {
        break;
      }
      Thread.sleep(1000);
    }

    //test single SSM
    boolean caughtException = false;
    try {
      conf.set(SSMConfigureKeys.DFS_SSM_RPC_ADDRESS_KEY, "localhost:8043");
      SSMServer.createSSM(null, conf);
    } catch (IOException e) {
      assertEquals("java.io.IOException: Another SSMServer is running",
          e.toString());
      caughtException = true;
    }
    assertTrue(caughtException);
  }
}