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
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.junit.Before;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;

public class TestErasureCodingActionBase extends MiniClusterHarness {
  ErasureCodingPolicy ecPolicy;

  @Before
  public void init() throws Exception {
    // use ErasureCodeConstants.XOR_2_1_SCHEMA
    ecPolicy = SystemErasureCodingPolicies.getPolicies().get(3);
    cluster = new MiniDFSCluster.Builder(smartContext.getConf()).
        numDataNodes(ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()).
        build();
  }

  public void createTestFile(String srcPath, long length) throws IOException {
    DFSTestUtil.createFile(dfs, new Path(srcPath), length, (short) 3, 0L);
  }
}
