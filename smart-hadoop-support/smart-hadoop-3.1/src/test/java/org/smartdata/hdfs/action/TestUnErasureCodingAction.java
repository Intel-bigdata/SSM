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

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestUnErasureCodingAction extends TestErasureCodingMiniCluster {

  @Test
  public void testUnEcActionForFile() throws Exception {
    String testDir = "/test_dir";
    dfs.mkdirs(new Path(testDir));
    dfs.setErasureCodingPolicy(new Path(testDir), ecPolicy.getName());
    // create test file, its EC policy should be consistent with parent dir, i.e., ecPolicy.
    String srcPath = testDir + "/ec_file";
    createTestFile(srcPath, 1000);
    dfsClient.setStoragePolicy(srcPath, "COLD");
    HdfsFileStatus srcFileStatus = dfsClient.getFileInfo(srcPath);
    assertEquals(dfsClient.getErasureCodingPolicy(srcPath), ecPolicy);

    UnErasureCodingAction unEcAction = new UnErasureCodingAction();
    unEcAction.setContext(smartContext);
    String ecTmpPath = "/ssm/ec_tmp/tmp_file";
    Map<String, String> args = new HashMap<>();
    args.put(HdfsAction.FILE_PATH, srcPath);
    args.put(ErasureCodingBase.EC_TMP, ecTmpPath);
    unEcAction.init(args);
    unEcAction.run();
    assertTrue(unEcAction.getExpectedAfterRun());
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(srcPath);
    assertNull(fileStatus.getErasureCodingPolicy());
    // Examine the consistency of file attributes.
    assertEquals(srcFileStatus.getLen(), fileStatus.getLen());
    assertEquals(srcFileStatus.getModificationTime(), fileStatus.getModificationTime());
    assertEquals(srcFileStatus.getAccessTime(), fileStatus.getAccessTime());
    assertEquals(srcFileStatus.getOwner(), fileStatus.getOwner());
    assertEquals(srcFileStatus.getGroup(), fileStatus.getGroup());
    assertEquals(srcFileStatus.getPermission(), fileStatus.getPermission());
    // UNDEF storage policy makes the converted file's storage type uncertain, so it is excluded.
    if (srcFileStatus.getStoragePolicy() != 0) {
      assertEquals(srcFileStatus.getStoragePolicy(), fileStatus.getStoragePolicy());
    }
  }

  @Test
  public void testUnEcActionForDir() throws Exception {
    String testDir = "/test_dir";
    dfs.mkdirs(new Path(testDir));
    dfs.setErasureCodingPolicy(new Path(testDir), ecPolicy.getName());
    assertEquals(dfsClient.getErasureCodingPolicy(testDir), ecPolicy);

    UnErasureCodingAction unEcAction = new UnErasureCodingAction();
    unEcAction.setContext(smartContext);
    Map<String, String> args = new HashMap<>();
    args.put(HdfsAction.FILE_PATH, testDir);
    unEcAction.init(args);
    unEcAction.run();
    assertNull(dfs.getErasureCodingPolicy(new Path(testDir)));

    // Create test file, its EC policy is expected to be replication.
    String srcPath = testDir + "/ec_file";
    createTestFile(srcPath, 1000);
    assertNull(dfs.getErasureCodingPolicy(new Path(srcPath)));
  }
}
