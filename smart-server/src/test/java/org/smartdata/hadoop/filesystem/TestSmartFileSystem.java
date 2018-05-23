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
package org.smartdata.hadoop.filesystem;

import com.google.gson.Gson;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.server.MiniSmartClusterHarness;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestSmartFileSystem extends MiniSmartClusterHarness {
  private SmartFileSystem smartFileSystem;

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    this.smartFileSystem = new SmartFileSystem();
    SmartConf conf = smartContext.getConf();
    Collection<URI> nameNodes = DFSUtil.getInternalNsRpcUris(conf);
    smartFileSystem.initialize(new ArrayList<>(
        nameNodes).get(0), smartContext.getConf());
    createSmallFiles();
  }

  private void createSmallFiles() throws Exception {
    Path smallFilePath = new Path("/test/small_files/");
    dfs.mkdirs(smallFilePath);
    Path containerPath = new Path("/test/container_files/");
    dfs.mkdirs(containerPath);
    for (int i = 0; i < 2; i++) {
      String fileName = "/test/small_files/file_" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = 8;
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
        "/test/small_files/container_file_6");
    smallFileCompactAction.init(args);
    smallFileCompactAction.run();
  }

  @Test
  public void testSmartFileSystem() throws Exception {
    waitTillSSMExitSafeMode();
    smartFileSystem.rename(new Path("/test/small_files/file_0"),
        new Path("/test/small_files/file_5"));
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_0"));
    Assert.assertTrue(dfsClient.exists("/test/small_files/file_5"));
    smartFileSystem.delete(new Path("/test/small_files/file_0"), false);
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_0"));
    smartFileSystem.delete(new Path("/test/small_files"), true);
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_1"));
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_5"));
  }
}
