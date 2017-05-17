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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ssm.protocol.SSMClient;
import org.apache.hadoop.ssm.protocol.SSMServiceState;
import org.apache.hadoop.ssm.sql.TestDBUtil;
import org.apache.hadoop.ssm.sql.Util;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;

public class TestEmptyMiniSSMCluster {
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected SSMServer ssm;
  protected String dbFile;
  protected String dbUrl;

  @Before
  public void setUp() throws Exception {
    conf = new SSMConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();

    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    List<URI> uriList = new ArrayList<>(namenodes);
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());
    conf.set(SSMConfigureKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY,
        uriList.get(0).toString());

    // Set db used
    dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    dbUrl = Util.SQLITE_URL_PREFIX + dbFile;
    conf.set(SSMConfigureKeys.DFS_SSM_DEFAULT_DB_URL_KEY, dbUrl);

    // rpcServer start in SSMServer
    ssm = SSMServer.createSSM(null, conf);
  }

  public void waitTillSSMExitSafeMode() throws Exception {
    SSMClient client = new SSMClient(conf);
    long start = System.currentTimeMillis();
    int retry = 5;
    while (true) {
      try {
        SSMServiceState state = client.getServiceState();
        if (state != SSMServiceState.SAFEMODE) {
          break;
        }
        int secs = (int)(System.currentTimeMillis() - start) / 1000;
        System.out.println("Waited for " + secs + " seconds ...");
        Thread.sleep(1000);
      } catch (Exception e) {
        if (retry <= 0) {
          throw e;
        }
        retry--;
      }
    }
  }

  @After
  public void cleanUp() {
    if (ssm != null) {
      ssm.shutdown();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
