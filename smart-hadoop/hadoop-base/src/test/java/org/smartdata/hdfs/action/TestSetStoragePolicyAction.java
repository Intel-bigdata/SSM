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

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.MockActionStatusReporter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for SetStoragePolicyAction.
 */
public class TestSetStoragePolicyAction extends ActionMiniCluster {
  private static final byte MEMORY_STORAGE_POLICY_ID = 15;
  private static final byte ALLSSD_STORAGE_POLICY_ID = 12;
  private static final byte ONESSD_STORAGE_POLICY_ID = 10;
  private static final byte HOT_STORAGE_POLICY_ID = 7;
  private static final byte WARM_STORAGE_POLICY_ID = 5;
  private static final byte COLD_STORAGE_POLICY_ID = 2;

  @Test
  public void testDifferentPolicies() throws IOException {
    final String file = "/testStoragePolicy/file";
    dfsClient.mkdirs("/testStoragePolicy");

    // write to HDFS
    final OutputStream out = dfsClient.create(file, true);
    byte[] content = "Hello".getBytes();
    out.write(content);
    out.close();

    byte policy = setStoragePolicy(file, "ALL_SSD");
    Assert.assertEquals(ALLSSD_STORAGE_POLICY_ID, policy);
    policy = setStoragePolicy(file, "COLD");
    Assert.assertEquals(COLD_STORAGE_POLICY_ID, policy);

    policy = setStoragePolicy(file, "ONE_SSD");
    Assert.assertEquals(ONESSD_STORAGE_POLICY_ID, policy);

    policy = setStoragePolicy(file, "HOT");
    Assert.assertEquals(HOT_STORAGE_POLICY_ID, policy);

    policy = setStoragePolicy(file, "WARM");
    Assert.assertEquals(WARM_STORAGE_POLICY_ID, policy);
  }

  private byte setStoragePolicy(String file, String storagePolicy)
      throws IOException {
    SetStoragePolicyAction action = new SetStoragePolicyAction();
    action.setDfsClient(dfsClient);
    action.setContext(smartContext);
    action.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap();
    args.put(SetStoragePolicyAction.FILE_PATH, file);
    args.put(SetStoragePolicyAction.STORAGE_POLICY, storagePolicy);
    action.init(args);
    action.run();
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(file);
    return fileStatus.getStoragePolicy();
  }
}
