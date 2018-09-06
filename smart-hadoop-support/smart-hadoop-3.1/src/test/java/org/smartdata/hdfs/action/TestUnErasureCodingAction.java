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

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class TestUnErasureCodingAction extends MiniClusterHarness {

  public void createTestFile(String srcPath) throws IOException {
    int bufferSize = 1024 * 1024;
    DFSOutputStream out =
        dfsClient.create(srcPath, FsPermission.getDefault(), EnumSet.of(CreateFlag.CREATE,
            CreateFlag.OVERWRITE, CreateFlag.SHOULD_REPLICATE), false, (short) 1,
            DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT, null, bufferSize, null,
            null, DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);
    for (int i = 0; i < 60; i++) {
      out.write(1);
    }
    out.close();
  }

  @Test
  public void testExecute()  throws Exception {
    UnErasureCodingAction unecAction = new UnErasureCodingAction();
    String srcPath = "/ec/ecfile";
    // create test file with default ecPolicy
    createTestFile(srcPath);
    String destPath = "/ssm/ec_tmp/tmp_file";
    Map<String, String> args = new HashMap<>();
    args.put(HdfsAction.FILE_PATH, srcPath);
    args.put(ErasureCodingBase.TMP_PATH, destPath);
    unecAction.init(args);
    Assert.assertTrue(unecAction.getExpectedAfterRun());
    Assert.assertEquals(dfsClient.getErasureCodingPolicy(srcPath),
        SystemErasureCodingPolicies.getReplicationPolicy());
    // compare attribute
  }
}
