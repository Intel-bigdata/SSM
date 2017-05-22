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
package org.apache.hadoop.smart;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.smart.protocol.SmartClient;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.rule.RuleState;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestSmartIntegration extends TestEmptyMiniSmartCluster {

  @Test
  public void test() throws Exception {
    waitTillSSMExitSafeMode();

    // Submit a rule
    SmartClient client = new SmartClient(conf);
    String rule = "file : every 1s | "
        + "accessCount(5s) >= 5 and length < 80 | cachefile";
    long ruleId = client.submitRule(rule, RuleState.ACTIVE);

    RuleInfo info = client.getRuleInfo(ruleId);
    System.out.println(info);
    Assert.assertTrue(info.getNumCmdsGen() == 0);

    // Create a file in HDFS
    DistributedFileSystem dfs = cluster.getFileSystem();

    String[] files = new String[] {
        "/fileA", "/fileB", "/fileC", "/fileD"
    };
    long[] lengths = new long[] {10, 20, 100, 200};
    int[] readCounts = new int[] {1, 10, 1, 10};

    for (int i = 0; i < files.length; i++) {
      DFSTestUtil.createFile(dfs, new Path(files[i]),
          lengths[i], (byte) 1, 2017);
    }

    Thread.sleep(2000);

    // Make sure the rule is working
    RuleInfo info2 = client.getRuleInfo(ruleId);
    Assert.assertTrue(info2.getNumChecked() - info.getNumChecked() >= 1);
    Assert.assertTrue(info.getNumCmdsGen() == 0);

    for (int i = 0; i < files.length; i++) {
      readFile(dfs, files[i], readCounts[i]);
    }
    Thread.sleep(6000);
    readFile(dfs, files[0], 1);

    RuleInfo info3 = client.getRuleInfo(ruleId);
    RuleInfo info4 = info3;
    int indexChange = 0;
    for (int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      info4 = client.getRuleInfo(ruleId);
      System.out.println("Time " + System.currentTimeMillis() + ":  " + info4);
      if (indexChange != 0 || info4.getNumCmdsGen() != 0) {
        indexChange++;
      }
    }
    long numCmdsGen = info4.getNumCmdsGen() - info3.getNumCmdsGen();
    Assert.assertTrue(numCmdsGen > 0 && numCmdsGen <= indexChange + 2);

    ssm.shutdown();
    ssm = null;

    // TODO: to be continued

  }

  private void readFile(DistributedFileSystem fs, String file, int times)
      throws Exception {
    for (int i = 0; i < times; i++) {
      DFSTestUtil.readFile(fs, new Path(file));
    }
  }
}
