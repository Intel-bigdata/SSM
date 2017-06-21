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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.cmdlet.CmdletInfo;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.server.metastore.MetaUtil;
import org.smartdata.server.metastore.TestDBUtil;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestSmartAdmin {

  @Test
  public void test() throws Exception {
    final SmartConf conf = new SmartConf();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).build();
    SmartServer server = null;
    SmartAdmin admin = null;

    try {
      // dfs not used , but datanode.ReplicaNotFoundException throws without dfs
      final DistributedFileSystem dfs = cluster.getFileSystem();

      final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      List<URI> uriList = new ArrayList<>(namenodes);
      conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());
      conf.set(SmartConfKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY,
          uriList.get(0).toString());

      // Set db used
      String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
      String dbUrl = MetaUtil.SQLITE_URL_PREFIX + dbFile;
      conf.set(SmartConfKeys.DFS_SSM_DB_URL_KEY, dbUrl);

      // rpcServer start in SmartServer
      server = SmartServer.launchWith(conf);
      admin = new SmartAdmin(conf);

      while (true) {
        //test getServiceStatus
        String state = admin.getServiceState().getName();
        if ("ACTIVE".equals(state)) {
          break;
        }
        Thread.sleep(1000);
      }

      //test listRulesInfo and submitRule
      List<RuleInfo> ruleInfos = admin.listRulesInfo();
      int ruleCounts0 = ruleInfos.size();
      long ruleId = admin.submitRule(
          "file: every 5s | path matches \"/foo*\"| cache",
          RuleState.DRYRUN);
      ruleInfos = admin.listRulesInfo();
      int ruleCounts1 = ruleInfos.size();
      assertEquals(1, ruleCounts1 - ruleCounts0);

      //test checkRule
      //if success ,no Exception throw
      admin.checkRule("file: every 5s | path matches \"/foo*\"| cache");
      boolean caughtException = false;
      try {
        admin.checkRule("file.path");
      } catch (IOException e) {
        caughtException = true;
      }
      assertTrue(caughtException);

      //test getRuleInfo
      RuleInfo ruleInfo = admin.getRuleInfo(ruleId);
      assertNotEquals(null, ruleInfo);

      //test disableRule
      admin.disableRule(ruleId, true);
      assertEquals(RuleState.DISABLED, admin.getRuleInfo(ruleId).getState());

      //test activateRule
      admin.activateRule(ruleId);
      assertEquals(RuleState.ACTIVE, admin.getRuleInfo(ruleId).getState());

      //test deleteRule
      admin.deleteRule(ruleId, true);
      assertEquals(RuleState.DELETED, admin.getRuleInfo(ruleId).getState());

      //test cmdletInfo
      long id = admin.submitCmdlet("cache -file /foo*");
      CmdletInfo cmdletInfo = admin.getCmdletInfo(id);
      assertTrue("cache -file /foo*".equals(cmdletInfo.getParameters()));

      //test actioninfo
      List<Long> aidlist = cmdletInfo.getAids();
      assertNotEquals(0,aidlist.size());
      ActionInfo actionInfo = admin.getActionInfo(aidlist.get(0));
      assertEquals(id,actionInfo.getCmdletId());

      //test listActionInfoOfLastActions
      admin.listActionInfoOfLastActions(2);

      //test client close
      admin.close();
      try {
        admin.getRuleInfo(ruleId);
        Assert.fail("Should fail because admin has closed.");
      } catch (IOException e) {
      }

      admin = null;
    } finally {
      if (admin != null) {
        admin.close();
      }

      if (server != null) {
        server.shutdown();
      }

      cluster.shutdown();
    }
  }
}