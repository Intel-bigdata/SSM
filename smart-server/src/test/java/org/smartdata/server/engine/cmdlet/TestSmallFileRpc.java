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
package org.smartdata.server.engine.cmdlet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.server.MiniSmartClusterHarness;

public class TestSmallFileRpc extends MiniSmartClusterHarness {

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    createMetaDate();
  }

  private void createMetaDate() throws Exception {
    MetaStore metaStore = ssm.getMetaStore();
    FileContainerInfo fileContainerInfo = new FileContainerInfo("/test/contain_file", 0, 10240);
    metaStore.insertSmallFile("/test/small_file", fileContainerInfo);
  }

  @Test
  public void testFileContainerInfoRpc() throws Exception {
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    FileContainerInfo fileContainerInfo = smartDFSClient.getFileContainerInfo("/test/small_file");
    Assert.assertEquals("/test/contain_file", fileContainerInfo.getContainerFilePath());
    Assert.assertEquals(0, fileContainerInfo.getOffset());
    Assert.assertEquals(10240, fileContainerInfo.getLength());
  }
}
