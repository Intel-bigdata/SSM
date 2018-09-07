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
package org.smartdata.hdfs.action;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestErasureCodingAction extends MiniClusterHarness {

  public void createTestFile(String srcPath) throws IOException {
    FSDataOutputStream out = dfs.create(new Path(srcPath));
    for (int i = 0; i < 60; i++) {
      out.writeByte(1);
    }
    out.close();
  }

  @Test
  public void testExecute()  throws Exception {
    ErasureCodingAction ecAction = new ErasureCodingAction();
    String srcPath = "/ec/unecfile";
    createTestFile(srcPath);
    String ecTmpPath = "/ssm/ec_tmp/tmp_file";
    String originTmpPath = "/ssm/origin_tmp/tmp_file";
    Map<String, String> args = new HashMap<>();
    args.put(HdfsAction.FILE_PATH, srcPath);
    args.put(ErasureCodingBase.EC_TMP, ecTmpPath);
    args.put(ErasureCodingBase.ORIGIN_TMP, originTmpPath);
    ecAction.init(args);
    Assert.assertTrue(ecAction.getExpectedAfterRun());
    Assert.assertEquals(dfsClient.getErasureCodingPolicy(srcPath),
        DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);
    // compare attribute
  }
}