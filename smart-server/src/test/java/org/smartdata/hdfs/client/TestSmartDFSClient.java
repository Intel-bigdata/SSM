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
package org.smartdata.hdfs.client;

import com.google.gson.Gson;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.server.MiniSmartClusterHarness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestSmartDFSClient extends MiniSmartClusterHarness {

  private void createSmallFiles() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    for (int i = 0; i < 3; i++) {
      String fileName = "/test/small_files/file_" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = 9;
      byte[] buf = new byte[50];
      Random rb = new Random(2018);
      rb.nextBytes(buf);
      out.write(buf, 0, (int) fileLen);
      out.close();
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
        "/test/small_files/container_file_3");
    smallFileCompactAction.init(args);
    smallFileCompactAction.run();
  }

  @Test
  public void testSmallFile() throws Exception {
    createSmallFiles();
    waitTillSSMExitSafeMode();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    BlockLocation[] blockLocations = smartDFSClient.getBlockLocations(
        "/test/small_files/file_0", 0, 30);
    Assert.assertEquals(blockLocations.length, 1);
    HdfsFileStatus fileInfo = smartDFSClient.getFileInfo(
        "/test/small_files/file_0");
    Assert.assertEquals(9, fileInfo.getLen());
    smartDFSClient.rename("/test/small_files/file_0", "/test/small_files/file_5");
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_0"));
    Assert.assertTrue(dfsClient.exists("/test/small_files/file_5"));
    smartDFSClient.delete("/test/small_files/file_5", false);
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_5"));
    smartDFSClient.delete("/test/small_files", true);
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_1"));
  }

  private void createCompressedFile() throws Exception {
    // TODO add create files
    Path path = new Path("/test/compress_files/");
    dfs.mkdirs(path);
    for (int i = 0; i < 3; i++) {
      String fileName = "/test/compress_files/file_" + i;
      DFSTestUtil.createFile(dfs, new Path(fileName),
          1024 * 1024, (short) 1, 1);
    }
  }

  @Test
  public void testCompressedFile() throws Exception {
    createCompressedFile();
    waitTillSSMExitSafeMode();
    // TODO add test cases
  }

  @After
  public void tearDown() throws Exception {
    dfs.getClient().delete("/test", true);
  }
}
