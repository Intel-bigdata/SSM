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
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletState;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.CmdletManager;

import java.util.ArrayList;
import java.util.Random;

public class TestSmartDFSClient extends MiniSmartClusterHarness {
  private SmartDFSClient smartDFSClient;

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    this.smartDFSClient = new SmartDFSClient(smartContext.getConf());
    createSmallFiles();
  }

  private void createSmallFiles() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    ArrayList<String> smallFileList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String fileName = "/test/small_files/file_" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = 8;
      byte[] buf = new byte[50];
      Random rb = new Random(2018);
      rb.nextBytes(buf);
      out.write(buf, 0, (int) fileLen);
      out.close();
      smallFileList.add(fileName);
    }
    waitTillSSMExitSafeMode();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    CmdletDescriptor cmdletDescriptor = CmdletDescriptor.fromCmdletString("compact -containerFile"
        + " /test/small_files/container_file");
    cmdletDescriptor.addActionArg(0, HdfsAction.FILE_PATH, new Gson().toJson(smallFileList));
    Thread.sleep(1000);
    long cmdId = cmdletManager.submitCmdlet(cmdletDescriptor);

    while (true) {
      Thread.sleep(1000);
      CmdletState state = cmdletManager.getCmdletInfo(cmdId).getState();
      if (state == CmdletState.DONE) {
        return;
      } else if (state == CmdletState.FAILED) {
        Assert.fail("Compact failed.");
      }
    }
  }

  @Test
  public void testGetLocatedBlocks() throws Exception {
    smartDFSClient.getLocatedBlocks("/test/small_files/file_0", 0);
  }

  @Test
  public void testGetBlockLocations() throws Exception {
    BlockLocation[] blockLocations = smartDFSClient.getBlockLocations(
        "/test/small_files/file_0", 0, 30);
    Assert.assertEquals(blockLocations.length, 1);
  }

  @Test
  public void testGetFileInfo() throws Exception {
    HdfsFileStatus fileInfo = smartDFSClient.getFileInfo(
        "/test/small_files/file_0");
    Assert.assertEquals(fileInfo.getLen(), 8);
  }

  @Test
  public void testDeleteFile() throws Exception {
    smartDFSClient.delete("/test/small_files/file_0", false);
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_0"));
  }

  @Test
  public void testDeleteFileRecur() throws Exception {
    smartDFSClient.delete("/test/small_files", true);
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_0"));
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_1"));
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_2"));
  }

  //@Test
  public void testTruncateFile() throws Exception {
    smartDFSClient.truncate("/test/small_files/file_0", 0);
    Thread.sleep(3000);
    Assert.assertEquals(0, smartDFSClient.getFileInfo(
        "/test/small_files/file_0").getLen());
  }

  @Test
  public void testRenameFile() throws Exception {
    smartDFSClient.rename("/test/small_files/file_0", "/test/small_files/file_5");
    Assert.assertTrue(!dfsClient.exists("/test/small_files/file_0"));
    Assert.assertTrue(dfsClient.exists("/test/small_files/file_5"));
  }
}
