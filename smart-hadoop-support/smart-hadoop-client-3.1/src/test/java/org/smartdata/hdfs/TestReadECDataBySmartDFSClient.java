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
package org.smartdata.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSStripedInputStream;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.action.TestErasureCodingMiniCluster;
import org.smartdata.hdfs.client.SmartDFSClient;

import java.io.IOException;

public class TestReadECDataBySmartDFSClient extends TestErasureCodingMiniCluster {
  public static final String TEST_DIR = "/ec";

  @Test
  public void testReadECData() throws IOException {

    cluster.getFileSystem().mkdirs(new Path(TEST_DIR));
    // Set an EC policy for this test dir, so the file created under it will
    // be stored by this EC policy.
    dfsClient.setErasureCodingPolicy(TEST_DIR, ecPolicy.getName());
    String srcPath = "/ec/a.txt";
    createTestFile(srcPath, 300000);
    Assert.assertTrue(ecPolicy == dfsClient.getErasureCodingPolicy(srcPath));

    SmartConf smartConf = smartContext.getConf();
    // The below single configuration is in order to make sure a SmartDFSClient can be created
    // successfully, and the actual value for this property does't matter.
    smartConf.set(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY,
        SmartConfKeys.SMART_SERVER_RPC_ADDRESS_DEFAULT);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartConf);
    DFSInputStream dfsInputStream = smartDFSClient.open(srcPath);
    // In unit test, a DFSInputStream can be still used to read EC data. But in real environment,
    // DFSStripedInputStream is required, otherwise, block not found exception will occur.
    Assert.assertTrue(dfsInputStream instanceof DFSStripedInputStream);
    int bufferSize = 64 * 1024;
    byte[] buffer = new byte[bufferSize];
    // Read EC data from HDFS
    while (dfsInputStream.read(buffer, 0, bufferSize) != -1) {
    }
    dfsInputStream.close();
  }
}
