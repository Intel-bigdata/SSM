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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class TestAppendFileAction extends MiniClusterHarness {
  private void appendFile(String src, long length) throws IOException {
    Map<String, String> args = new HashMap<>();
    args.put(AppendFileAction.FILE_PATH, src);
    args.put(AppendFileAction.LENGTH, "" + length);
    AppendFileAction appendFileAction = new AppendFileAction();
    appendFileAction.setDfsClient(dfsClient);
    appendFileAction.setContext(smartContext);
    appendFileAction.init(args);
    appendFileAction.setConf(smartContext.getConf());
    appendFileAction.run();
    Assert.assertTrue(appendFileAction.getExpectedAfterRun());
  }

  @Test
  public void testInit() throws IOException {
    Map<String, String> args = new HashMap<>();
    args.put(AppendFileAction.FILE_PATH, "/Test");
    args.put(AppendFileAction.LENGTH, "100000000000000");
    AppendFileAction appendFileAction = new AppendFileAction();
    appendFileAction.init(args);
    args.put(AppendFileAction.BUF_SIZE, "1024");
    appendFileAction.init(args);
  }

  @Test
  public void testAppendNonExistFile() throws UnsupportedEncodingException {
    Map<String, String> args = new HashMap<>();
    args.put(WriteFileAction.FILE_PATH, "/Test");
    AppendFileAction appendFileAction = new AppendFileAction();
    appendFileAction.init(args);
    appendFileAction.run();
    Assert.assertNotNull(appendFileAction.getActionStatus().getThrowable());
  }

  @Test
  public void testAppendFile() throws IOException {
    Path src = new Path("/srcFile");
    int size = 1024;
    int appendLength = 1024;
    DFSTestUtil.createFile(dfs, src, size, (short)3, 0xFEED);
    appendFile("/srcFile", 1024);
    FileStatus fileStatus = dfs.getFileStatus(src);
    Assert.assertEquals(size + appendLength, fileStatus.getLen());
  }
}
