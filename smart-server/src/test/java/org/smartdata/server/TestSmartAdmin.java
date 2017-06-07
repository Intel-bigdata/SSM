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
import org.junit.Test;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.command.CommandInfo;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.Util;

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
      String dbUrl = Util.SQLITE_URL_PREFIX + dbFile;
      conf.set(SmartConfKeys.DFS_SSM_DEFAULT_DB_URL_KEY, dbUrl);

      // rpcServer start in SmartServer
      server = SmartServer.createSSM(null, conf);
      SmartAdmin ssmClient = new SmartAdmin(conf);

      while (true) {
        //test getServiceStatus
        String state = ssmClient.getServiceState().getName();
        if ("ACTIVE".equals(state)) {
          break;
        }
        Thread.sleep(1000);
      }

      //test listRulesInfo and submitRule
      List<RuleInfo> ruleInfos = ssmClient.listRulesInfo();
      int ruleCounts0 = ruleInfos.size();
      long ruleId = ssmClient.submitRule(
          "file: every 5s | path matches \"/foo*\"| cache",
          RuleState.DRYRUN);
      ruleInfos = ssmClient.listRulesInfo();
      int ruleCounts1 = ruleInfos.size();
      assertEquals(1, ruleCounts1 - ruleCounts0);

      //test checkRule
      //if success ,no Exception throw
      ssmClient.checkRule("file: every 5s | path matches \"/foo*\"| cache");
      boolean caughtException = false;
      try {
        ssmClient.checkRule("file.path");
      } catch (IOException e) {
        caughtException = true;
      }
      assertTrue(caughtException);

      //test getRuleInfo
      RuleInfo ruleInfo = ssmClient.getRuleInfo(ruleId);
      assertNotEquals(null, ruleInfo);

      //test disableRule
      ssmClient.disableRule(ruleId, true);
      assertEquals(RuleState.DISABLED, ssmClient.getRuleInfo(ruleId).getState());

      //test activateRule
      ssmClient.activateRule(ruleId);
      assertEquals(RuleState.ACTIVE, ssmClient.getRuleInfo(ruleId).getState());

      //test deleteRule
      ssmClient.deleteRule(ruleId, true);
      assertEquals(RuleState.DELETED, ssmClient.getRuleInfo(ruleId).getState());

      //test single SSM
      caughtException = false;
      try {
        conf.set(SmartConfKeys.DFS_SSM_RPC_ADDRESS_KEY, "localhost:8043");
        SmartServer.createSSM(null, conf);
      } catch (IOException e) {
        assertEquals("java.io.IOException: Another SmartServer is running",
            e.toString());
        caughtException = true;
      }
      assertTrue(caughtException);

      //test commandInfo
      long id = ssmClient.submitCommand("cache /foo*");
      CommandInfo commandInfo = ssmClient.getCommandInfo(id);
      assertTrue("cache /foo*".equals(commandInfo.getParameters()));

      //test actioninfo
      List<Long> aidlist = commandInfo.getAids();
      assertNotEquals(0,aidlist.size());
      ActionInfo actionInfo = ssmClient.getActionInfo(aidlist.get(0));
      assertEquals(id,actionInfo.getCommandId());

      //test listActionInfoOfLastActions
      ssmClient.listActionInfoOfLastActions(2);

      //test client close
      caughtException = false;
      ssmClient.close();
      try {
        ssmClient.getRuleInfo(ruleId);
      } catch (IOException e) {
        caughtException = true;
      }
      assertEquals(true, caughtException);
      server.shutdown();
      cluster.shutdown();
    } finally {
      if (server != null) {
        server.shutdown();
      }

      cluster.shutdown();
    }
  }

//  @Test
//  public void testReal() throws Exception {
//    final SmartConf conf = new SmartConf();
//
//    String[] args = new String[] {
//        "-D",
//        "dfs.smart.namenode.rpcserver=hdfs://localhost:9000"
//    };
//
//    // rpcServer start in SmartServer
//    SmartServer server = SmartServer.createSSM(args, conf);
//
//    Thread.sleep(6000);
//
//    SmartAdmin admin = new SmartAdmin(conf);
//    String rule = "file : every 1s | mtime > now - 70day | cache";
//    String cmd = "read /hadoopdbg";
//    long id = admin.submitCommand(cmd);
//
//    Thread.sleep(1000);
//    Thread.sleep(100000);
//  }
}