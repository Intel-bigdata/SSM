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

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestWriteFileAction extends MiniClusterHarness {
  protected void writeFile(String filePath, long length) throws IOException {
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.setDfsClient(dfsClient);
    writeFileAction.setContext(smartContext);
    writeFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(WriteFileAction.FILE_PATH, filePath);
    args.put(WriteFileAction.LENGTH, "" + length);
    writeFileAction.init(args);
    writeFileAction.run();
  }

  @Test
  public void testInit() throws IOException {
    Map<String, String> args = new HashMap<>();
    args.put(WriteFileAction.FILE_PATH, "/Test");
    args.put(WriteFileAction.LENGTH, "100000000000000");
    WriteFileAction writeFileAction = new WriteFileAction();
    writeFileAction.init(args);
    writeFileAction.setStatusReporter(new MockActionStatusReporter());
    args.put(WriteFileAction.BUF_SIZE, "1024");
    writeFileAction.init(args);
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
