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
package org.smartdata.integration;

import com.google.gson.Gson;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.util.VersionInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.server.MiniSmartClusterHarness;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestSmallFileRead extends MiniSmartClusterHarness {
  private int ret;
  private long fileLength;

  private void createTestFiles() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    for (int i = 0; i < 2; i++) {
      String fileName = "/test/small_files/file_" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = 5 + (int) (Math.random() * 11);
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
      if (i == 0) {
        fileLength = fileLen;
        ret = buf[0] & 0xff;
      }
    }

    SmallFileCompactAction smallFileCompactAction = new SmallFileCompactAction();
    smallFileCompactAction.setDfsClient(dfsClient);
    smallFileCompactAction.setContext(smartContext);
    Map<String , String> args = new HashMap<>();
    List<String> smallFileList = new ArrayList<>();
    smallFileList.add("/test/small_files/file_0");
    smallFileList.add("/test/small_files/file_1");
    args.put(SmallFileCompactAction.FILE_PATH , new Gson().toJson(smallFileList));
    args.put(SmallFileCompactAction.CONTAINER_FILE,
        "/test/small_files/container_file_5");
    smallFileCompactAction.init(args);
    smallFileCompactAction.run();
  }

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    createTestFiles();
  }

  @Test
  public void testRead() throws Exception {
    waitTillSSMExitSafeMode();
    // Not support hdfs3.x temporarily
    String version = VersionInfo.getVersion();
    String[] parts = version.split("\\.");
    if (Integer.valueOf(parts[0]) == 3) {
      return;
    }
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open("/test/small_files/file_0");
    Assert.assertEquals(1, is.getAllBlocks().size());
    Assert.assertEquals(fileLength, is.getFileLength());
    Assert.assertEquals(0, is.getPos());
    int byteRead = is.read();
    Assert.assertEquals(ret, byteRead);
    byte[] bytes = new byte[50];
    Assert.assertEquals(fileLength - 1, is.read(bytes));
    is.close();
    is = smartDFSClient.open("/test/small_files/file_0");
    ByteBuffer buffer = ByteBuffer.allocate(50);
    Assert.assertEquals(fileLength, is.read(buffer));
    is.close();
    is = smartDFSClient.open("/test/small_files/file_0");
    Assert.assertEquals(fileLength - 2, is.read(2, bytes, 1, 50));
    is.close();
  }

  @After
  public void tearDown() throws Exception {
    dfs.getClient().delete("/test", true);
  }
}
