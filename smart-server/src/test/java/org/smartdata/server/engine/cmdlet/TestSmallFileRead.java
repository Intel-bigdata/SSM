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
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletState;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.CmdletManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

public class TestSmallFileRead extends MiniSmartClusterHarness {
  private long fileLength;
  private int ret;

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    createSmallFiles();
  }

  private void createSmallFiles() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    ArrayList<String> smallFileList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String fileName = "/test/small_files/file_" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen = 5 + (int) (Math.random() * 11);
      byte[] buf = new byte[50];
      Random rb = new Random(2018);
      rb.nextBytes(buf);
      out.write(buf, 0, (int) fileLen);
      out.close();
      if (i == 1) {
        fileLength = fileLen;
        ret = buf[0] & 0xff;
      }
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
  public void testBasicInfo() throws Exception {
    waitTillSSMExitSafeMode();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open("/test/small_files/file_1");
    Assert.assertEquals(1, is.getAllBlocks().size());
    Assert.assertEquals(fileLength, is.getFileLength());
    Assert.assertEquals(0, is.getPos());
    is.close();
  }

  //@Test
  public void testByteRead() throws Exception {
    waitTillSSMExitSafeMode();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open("/test/small_files/file_1");
    int byteRead = is.read();
    Assert.assertEquals(ret, byteRead);
    is.close();
  }

  //@Test
  public void testByteArrayRead() throws Exception {
    waitTillSSMExitSafeMode();
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open("/test/small_files/file_1");
    byte[] bytes = new byte[50];
    Assert.assertEquals(fileLength, is.read(bytes));
    is.close();
  }

  //@Test
  public void testByteBufferRead() throws Exception {
    waitTillSSMExitSafeMode();
    String smallFile = "/test/small_files/file_1";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    ByteBuffer buffer = ByteBuffer.allocate(50);
    Assert.assertEquals(fileLength, is.read(buffer));
    is.close();
  }

  //@Test
  public void testBytesRead() throws Exception {
    waitTillSSMExitSafeMode();
    String smallFile = "/test/small_files/file_2";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    byte[] bytes = new byte[50];
    Assert.assertEquals(5, is.read(bytes, 1, 5));
    is.close();
  }

  //@Test
  public void testPositionRead() throws Exception {
    waitTillSSMExitSafeMode();
    String smallFile = "/test/small_files/file_1";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    DFSInputStream is = smartDFSClient.open(smallFile);
    byte[] bytes = new byte[50];
    Assert.assertEquals(fileLength - 2, is.read(2, bytes, 1, 50));
    is.close();
  }
}
