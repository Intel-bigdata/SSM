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
package org.smartdata.server.rule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.common.rule.RuleState;
import org.smartdata.server.SmartServer;
import org.smartdata.server.metastore.sql.TestDBUtil;
import org.smartdata.server.metastore.sql.Util;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;

public class TestSubmitRule {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private SmartServer ssm;

  @Before
  public void setUp() throws Exception {
    conf = new SmartConf();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();

    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    List<URI> uriList = new ArrayList<>(namenodes);
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());
    conf.set(SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY,
        uriList.get(0).toString());

    // Set db used
    String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    String dbUrl = Util.SQLITE_URL_PREFIX + dbFile;
    conf.set(SmartConfKeys.DFS_SSM_DEFAULT_DB_URL_KEY, dbUrl);

    // rpcServer start in SmartServer
    ssm = SmartServer.createSSM(null, conf);
  }

  @After
  public void cleanUp() throws Exception {
    ssm.stop();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSubmitRule() throws Exception {
    String rule = "file: every 1s \n | length > 10 | cachefile";
    SmartAdmin client = new SmartAdmin(conf);

    long ruleId = 0l;
    boolean wait = true;
    while (wait) {
      try {
        ruleId = client.submitRule(rule, RuleState.ACTIVE);
        wait = false;
      } catch (IOException e) {
        if (!e.toString().contains("not ready")) {
          throw e;
        }
      }
    }

    for (int i = 0; i < 10; i++) {
      long id = client.submitRule(rule, RuleState.ACTIVE);
      Assert.assertTrue(ruleId + i + 1 == id);
    }

    String badRule = "something else";
    try {
      client.submitRule(badRule, RuleState.ACTIVE);
      Assert.fail("Should have an exception here");
    } catch (IOException e) {
    }

    try {
      client.checkRule(badRule);
      Assert.fail("Should have an exception here");
    } catch (IOException e) {
    }
  }
}
