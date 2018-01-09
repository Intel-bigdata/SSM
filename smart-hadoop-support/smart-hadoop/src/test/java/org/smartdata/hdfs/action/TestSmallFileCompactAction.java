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

import com.google.gson.Gson;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestSmallFileCompactAction extends MiniClusterHarness {
  @Test
  public void testSmallFileCompactAction() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    long sumFileLen = 0L;
    List smallFileList = new ArrayList<String>();
    for (int i = 0; i < 3; i++) {
      String fileName = "/test/small_files/file_" + i;
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

    Assert.assertTrue(dfsClient.exists("/test/small_files/file_0"));
    Assert.assertTrue(dfsClient.exists("/test/small_files/file_1"));
    Assert.assertTrue(dfsClient.exists("/test/small_files/file_2"));

    SmallFileCompactAction smallFileCompactAction = new SmallFileCompactAction();
    smallFileCompactAction.setDfsClient(dfsClient);
    smallFileCompactAction.setContext(smartContext);
    smallFileCompactAction.setStatusReporter(new MockActionStatusReporter());
    Map<String , String> args = new HashMap<>();
    args.put(SmallFileCompactAction.FILE_PATH , new Gson().toJson(smallFileList));
    args.put(SmallFileCompactAction.CONTAINER_FILE, "/test/small_files/container_file");
    smallFileCompactAction.init(args);
    smallFileCompactAction.run();
    Assert.assertTrue(dfsClient.exists("/test/small_files/container_file"));
    HdfsFileStatus containerFileInfo = dfsClient.getFileInfo("/test/small_files/container_file");
    Assert.assertEquals(sumFileLen, containerFileInfo.getLen());
  }
}
