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

import com.google.gson.Gson;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletState;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.CmdletManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestSmallFileRead extends MiniSmartClusterHarness {
  private long fileLength;

  @Before
  @Override
  public void init() throws Exception {
    super.init();
  }

  private void createMetaData() throws Exception {
    MetaStore metaStore = ssm.getMetaStore();
    FileContainerInfo fileContainerInfo_1 = new FileContainerInfo("/test/contain_file", 0, 10240);
    FileContainerInfo fileContainerInfo_2 = new FileContainerInfo("/test/contain_file", 10240, 10240);
    metaStore.insertSmallFile("/test/small_file_1", fileContainerInfo_1);
    metaStore.insertSmallFile("/test/small_file_2", fileContainerInfo_2);
  }

  @Test
  public void testGetFileContainerInfoRpc() throws Exception {
    createMetaData();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    FileContainerInfo fileContainerInfo = smartDFSClient.getFileContainerInfo("/test/small_file_1");
    Assert.assertEquals("/test/contain_file", fileContainerInfo.getContainerFilePath());
    Assert.assertEquals(0, fileContainerInfo.getOffset());
    Assert.assertEquals(10240, fileContainerInfo.getLength());
  }

  @Test
  public void testGetSmallFileListRpc() throws Exception {
    createMetaData();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    List<String> smallFileList = smartDFSClient.getSmallFileList();
    Assert.assertEquals(2, smallFileList.size());
    Assert.assertEquals("/test/small_file_1", smallFileList.get(0));
    Assert.assertEquals("/test/small_file_2", smallFileList.get(1));
  }

  private void createSmallFiles() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    ArrayList<String> smallFileList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String fileName = "/test/small_files/file_" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = 40 + (int) (Math.random() * 11);
      byte[] buf = new byte[50];
      Random rb = new Random(2018);
      int bytesRemaining = (int) fileLen;
      while (bytesRemaining > 0) {
        rb.nextBytes(buf);
        int bytesToWrite = (bytesRemaining < buf.length) ? bytesRemaining : buf.length;
        out.write(buf, 0, bytesToWrite);
        bytesRemaining -= bytesToWrite;
      }
      out.close();
      if (i == 1) {
        fileLength = fileLen;
      }
      smallFileList.add(fileName);
    }
    waitTillSSMExitSafeMode();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    CmdletDescriptor cmdletDescriptor = CmdletDescriptor.fromCmdletString("compact -containerFile " +
        "/test/small_files/container_file");
    cmdletDescriptor.addActionArg(0, SmallFileCompactAction.SMALL_FILES, new Gson().toJson(smallFileList));
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
  public void testGetAllBlocks() throws Exception {
    createSmallFiles();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open("/test/small_files/file_0");
    Assert.assertEquals(1, is.getAllBlocks().size());
    is.close();
  }

  @Test
  public void testGetFileLen() throws Exception {
    createSmallFiles();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open("/test/small_files/file_1");
    Assert.assertEquals(fileLength, is.getFileLength());
    is.close();
  }

  @Test
  public void testGetPos() throws Exception {
    String smallFile = "/test/small_files/file_01";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    Assert.assertEquals(0, is.getPos());
    is.close();
  }

  @Test
  public void testByteRead() throws Exception {
    String smallFile = "/test/small_files/file_0";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    Assert.assertEquals(158, is.read());
    is.close();
  }

  @Test
  public void testByteArrayRead() throws Exception {
    String smallFile = "/test/small_files/file_0";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    byte[] bytes = new byte[20480];
    Assert.assertEquals(fileLength, is.read(bytes));
    is.close();
  }

  @Test
  public void testByteBuffer() throws Exception {
    String smallFile = "/test/small_files/file_0";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    ByteBuffer buffer = ByteBuffer.allocate(20480);
    Assert.assertEquals(fileLength, is.read(buffer));
    is.close();
  }

  @Test
  public void testBytesRead() throws Exception {
    String smallFile = "/test/small_files/file_0";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    byte[] bytes = new byte[100];
    Assert.assertEquals(5, is.read(bytes, 1, 5));
    is.close();
  }

  @Test
  public void testPositionRead() throws Exception {
    String smallFile = "/test/small_files/file_0";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    byte[] bytes = new byte[100];
    Assert.assertEquals(5, is.read(2, bytes, 1, 5));
    is.close();
  }
}
