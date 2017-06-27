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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.common.SmartServiceState;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.utils.MetaUtil;
import org.smartdata.metastore.utils.TestDBUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;

public class TestSmartServer {
  protected SmartConf conf;
  protected SmartServer ssm;
  protected String dbFile;
  protected String dbUrl;

  private static final int DEFAULT_BLOCK_SIZE = 100;

  static {
    TestBalancer.initTestSetup();
  }

  @Before
  public void setUp() throws Exception {
    conf = new SmartConf();
    initConf(conf);

    // Set db used
    dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    dbUrl = MetaUtil.SQLITE_URL_PREFIX + dbFile;
    conf.set(SmartConfKeys.DFS_SSM_DB_URL_KEY, dbUrl);

    // rpcServer start in SmartServer
    ssm = SmartServer.launchWith(conf);
  }

  private void initConf(Configuration conf) {

  }

  @Test
  public void test() {

  }

  @After
  public void cleanUp() {
    if (ssm != null) {
      ssm.shutdown();
    }
  }
}
