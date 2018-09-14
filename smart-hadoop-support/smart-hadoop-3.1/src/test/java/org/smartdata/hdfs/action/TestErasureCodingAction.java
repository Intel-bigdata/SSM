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

import org.apache.hadoop.fs.Path;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestErasureCodingAction extends TestErasureCodingMiniCluster {

  @Test
  public void testEcActionForFile() throws Exception {
    String srcPath = "/ec/test_file";
    createTestFile(srcPath, 1000);
    // the file is stored in replication
    assertEquals(null, dfsClient.getErasureCodingPolicy(srcPath));

    ErasureCodingAction ecAction = new ErasureCodingAction();
    ecAction.setContext(smartContext);
    String ecTmpPath = "/ssm/ec_tmp/tmp_file";
    Map<String, String> args = new HashMap<>();
    args.put(HdfsAction.FILE_PATH, srcPath);
    args.put(ErasureCodingBase.EC_TMP, ecTmpPath);
    args.put(ErasureCodingAction.EC_POLICY_NAME, ecPolicy.getName());
    ecAction.init(args);
    ecAction.run();
    assertTrue(ecAction.getExpectedAfterRun());
    // the file is stored in ec with default policy
    assertEquals(dfsClient.getErasureCodingPolicy(srcPath), ecPolicy);
  }

  @Test
  public void testEcActionForDir() throws Exception {
    String srcDirPath = "/test_dir/";
    dfs.mkdirs(new Path(srcDirPath));
    assertEquals(null, dfsClient.getErasureCodingPolicy(srcDirPath));

    ErasureCodingAction ecAction = new ErasureCodingAction();
    ecAction.setContext(smartContext);
    Map<String, String> args = new HashMap<>();
    args.put(HdfsAction.FILE_PATH, srcDirPath);
    args.put(ErasureCodingAction.EC_POLICY_NAME, ecPolicy.getName());
    ecAction.init(args);
    ecAction.run();
    assertTrue(ecAction.getExpectedAfterRun());
    assertEquals(dfsClient.getErasureCodingPolicy(srcDirPath), ecPolicy);

    String srcFilePath = "/test_dir/test_file";
    createTestFile(srcFilePath, 1000);
    // The newly created file should has the same EC policy as parent directory.
    assertEquals(dfsClient.getErasureCodingPolicy(srcFilePath), ecPolicy);
  }
}