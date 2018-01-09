/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.CmdletState;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.CmdletManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestSmallFileScheduler extends MiniSmartClusterHarness {
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
  }

  @Test
  public void testScheduler() throws Exception {
    waitTillSSMExitSafeMode();

    CmdletManager cmdletManager = ssm.getCmdletManager();
    CmdletDescriptor cmdletDescriptor = CmdletDescriptor.fromCmdletString("compact -containerFile "
        + "/test/small_files/container_file");
    cmdletDescriptor.addActionArg(0, HdfsAction.FILE_PATH, new Gson().toJson(smallFileList));
    long cmdId = cmdletManager.submitCmdlet(cmdletDescriptor);

    while (true) {
      Thread.sleep(3000);
      CmdletState state = cmdletManager.getCmdletInfo(cmdId).getState();
      if (state == CmdletState.DONE) {
        MetaStore metaStore = ssm.getMetaStore();
        long containerFileLen = metaStore.getFile("/test/small_files/container_file").getLength();
        Assert.assertEquals(sumFileLen, containerFileLen);
        long smallFileLen = metaStore.getFile("/test/small_files/file_1").getLength();
        Assert.assertEquals(0, smallFileLen);
        return;
      } else if (state == CmdletState.FAILED) {
        Assert.fail("Compact failed.");
      }
    }
  }
}
