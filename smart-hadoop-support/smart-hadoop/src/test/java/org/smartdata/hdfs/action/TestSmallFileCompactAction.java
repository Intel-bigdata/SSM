/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs.action;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestSmallFileCompactAction extends MiniClusterHarness {
  private long sumFileLen = 0L;

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    createTestFiles();
  }

  public void createTestFiles() throws Exception {
    Path path = new Path("/test/smallfile/");
    dfs.mkdirs(path);

    for (int i = 0; i < 10; i++) {
      String fileName = "/test/smallfile/file" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = (10 + (int) (Math.random() * 11)) * 1024;
      byte[] toWrite = new byte[1024];
      Random rb = new Random(2017);
      long bytesToWrite = fileLen;
      while (bytesToWrite > 0) {
        rb.nextBytes(toWrite);
        int bytesToWriteNext = (1024 < bytesToWrite) ? 1024 : (int) bytesToWrite;
        out.write(toWrite, 0, bytesToWriteNext);
        bytesToWrite -= bytesToWriteNext;
      }
      sumFileLen += fileLen;
      out.close();
      Assert.assertTrue(dfsClient.exists(fileName));
    }
  }

  @Test
  public void testSmallFileCompact() throws Exception {
    SmallFileCompactAction smallFileCompactAction = new SmallFileCompactAction();
    smallFileCompactAction.setDfsClient(dfsClient);
    smallFileCompactAction.setContext(smartContext);
    Map<String, String> args = new HashMap<>();
    args.put(SmallFileCompactAction.FILE_PATH, "/test/smallfile/");
    smallFileCompactAction.init(args);
    smallFileCompactAction.execute();
    //Check if file is not exist
    Assert.assertTrue(dfsClient.exists("/test/testFile"));
    Assert.assertEquals(dfsClient.open("/test/testFile").getFileLength(), sumFileLen);
  }
}
