/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ssm;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.ssm.protocol.SSMClient;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestSSMClient {

  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";

  @Test
  public void test() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION_KEY);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).build();
    final DistributedFileSystem dfs = cluster.getFileSystem();

    String ADDRESS = "localhost";
    int port = 9998;
    InetSocketAddress addr = new InetSocketAddress(ADDRESS, port);

    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    List<URI> uriList = new ArrayList<>(namenodes);

    // rpcServer start in SSMServer
    SSMServer ssmServer = SSMServer.createSSM(null, uriList.get(0), new Configuration());
    SSMClient ssmClient = new SSMClient(new Configuration(), addr);
    String state = ssmClient.getServiceStatus().getState().name();
    assertTrue("SAFEMODE".equals(state));
    assertNotEquals(null, ssmServer.getOut());
    cluster.close();
  }
}