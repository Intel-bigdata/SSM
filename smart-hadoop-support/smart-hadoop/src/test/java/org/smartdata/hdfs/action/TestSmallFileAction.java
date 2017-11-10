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

import com.google.gson.Gson;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.MiniClusterHarness;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.HashMap;
import java.util.Map;

public class TestSmallFileAction extends MiniClusterHarness {
  private long sumFileLen;
  private List<String> smallFileList;

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    sumFileLen = 0L;
    smallFileList = new ArrayList<>();
    createTestFiles();
  }

  private void createTestFiles() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    for (int i = 0; i < 10; i++) {
      String fileName = "/test/small_files/file" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = 10 + (int) (Math.random() * 11);
      byte[] buf = new byte[20];
      Random rb = new Random(2018);
      int bytesRemaining = (int) fileLen;
      while (bytesRemaining > 0) {
        rb.nextBytes(buf);
        int bytesToWrite = (bytesRemaining < buf.length) ? bytesRemaining : buf.length;
        out.write(buf, 0, bytesToWrite);
        bytesRemaining -= bytesToWrite;
      }
      out.close();
      sumFileLen += fileLen;
      smallFileList.add(fileName);
    }
  }

  @Test
  public void testSmallFileCompact() throws Exception {
    SmallFileCompactAction smallFileCompactAction = new SmallFileCompactAction();
    smallFileCompactAction.setDfsClient(dfsClient);
    smallFileCompactAction.setContext(smartContext);
    Map<String, String> args = new HashMap<>();
    args.put(SmallFileCompactAction.SMALL_FILES, new Gson().toJson(smallFileList));
    args.put(SmallFileCompactAction.CONTAINER_FILE, "/test/small_files/container_file");
    smallFileCompactAction.init(args);
    smallFileCompactAction.execute();
    // Check if file exists and length match
    Assert.assertTrue(dfsClient.exists("/test/small_files/container_file"));
    Assert.assertEquals(dfsClient.open("/test/small_files/container_file").getFileLength(), sumFileLen);
  }
}
