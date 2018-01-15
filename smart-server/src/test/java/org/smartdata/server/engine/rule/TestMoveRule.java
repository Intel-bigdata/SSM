///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.smartdata.server.engine.rule;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.DFSTestUtil;
//import org.junit.Test;
//import org.smartdata.admin.SmartAdmin;
//import org.smartdata.model.ActionInfo;
//import org.smartdata.model.RuleState;
//import org.smartdata.server.MiniSmartClusterHarness;
//
//import java.util.List;
//
//public class TestMoveRule extends MiniSmartClusterHarness {
//
//  @Test
//  public void testMoveDir() throws Exception {
//    waitTillSSMExitSafeMode();
//
//    dfs.mkdirs(new Path("/test"));
//    dfs.setStoragePolicy(new Path("/test"), "HOT");
//    dfs.mkdirs(new Path("/test/dir1"));
//    DFSTestUtil.createFile(dfs, new Path("/test/dir1/f1"), DEFAULT_BLOCK_SIZE * 3, (short) 3, 0);
//
//    String rule = "file: path matches \"/test/*\" | allssd";
//    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
//
//    long ruleId = admin.submitRule(rule, RuleState.ACTIVE);
//
//    int idx = 0;
//    while (idx++ < 6) {
//      Thread.sleep(1000);
//      List<ActionInfo> infos = admin.listActionInfoOfLastActions(100);
//      System.out.println(idx + " round:");
//      for (ActionInfo info : infos) {
//        System.out.println("\t" + info);
//      }
//      System.out.println();
//    }
//  }
//}
