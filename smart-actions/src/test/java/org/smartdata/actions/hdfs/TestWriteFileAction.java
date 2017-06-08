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
package org.smartdata.actions.hdfs;

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.ActionStatus;

import java.io.IOException;
import java.util.ArrayList;

public class TestWriteFileAction extends ActionMiniCluster {
  protected void writeFile(String filePath, long length) throws IOException {
    String[] args = {filePath, String.valueOf(length)};
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.setDfsClient(dfsClient);
    writeFileAction.setContext(smartContext);
    writeFileAction.init(args);
    writeFileAction.run();

    // check results
    ActionStatus actionStatus = writeFileAction.getActionStatus();
    Assert.assertTrue(actionStatus.isFinished());
    Assert.assertTrue(actionStatus.isSuccessful());
    System.out.println("Write file action running time : " +
        StringUtils.formatTime(actionStatus.getRunningTime()));
    Assert.assertEquals(1.0f, actionStatus.getPercentage(), 0.00001f);
  }

  @Test
  public void testInit() throws IOException {
    ArrayList<String> args = new ArrayList<>();
    args.add("/Test");
    args.add("100000000000000");
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.init(args.toArray(new String[args.size()]));
    args.add("1024");
    writeFileAction.init(args.toArray(new String[args.size()]));
  }

  @Test
  public void testExecute() throws Exception {
    String filePath = "/testWriteFile/fadsfa/213";
    long size = 10000;
    writeFile(filePath, size);
    HdfsFileStatus fileStatus = dfs.getClient().getFileInfo(filePath);
    Assert.assertTrue(fileStatus.getLen() == size);
  }
}
