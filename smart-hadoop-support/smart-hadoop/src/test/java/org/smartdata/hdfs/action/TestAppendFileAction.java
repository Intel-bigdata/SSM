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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for AppendFileAction.
 */
public class TestAppendFileAction extends MiniClusterHarness {

  @Test
  public void testLocalFileAppend() throws Exception {
    final String srcPath = "/testAppend";
    final String file1 = "file1";
    Path srcDir = new Path(srcPath);
    dfs.mkdirs(srcDir);

    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));

    for (int i = 0; i < 50; i++) {
      out1.write(2);
    }

    out1.close();

    AppendFileAction appendFileAction = new AppendFileAction();
    appendFileAction.setDfsClient(dfsClient);
    appendFileAction.setContext(smartContext);
    appendFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String,String> args = new HashMap<>();
    args.put(AppendFileAction.FILE_PATH,srcPath+"/"+file1);
    appendFileAction.init(args);
    appendFileAction.run();

    FSDataInputStream inputStream = dfs.open(new Path(srcPath+"/"+file1));

    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(inputStream.readByte()==2);
    }

    for (int i = 0; i < 40; i++) {
      Assert.assertTrue(inputStream.readByte()==1);
    }
  }

  @Test
  public void testRemoteFileAppend() throws Exception {
    final String srcPath = "/testAppend";
    final String file1 = "file1";
    String srcDir = dfs.getUri() + srcPath;
    Path path = new Path(srcDir);
    dfs.mkdirs(path);

    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    for (int i = 0; i < 50; i++) {
      out1.write(2);
    }

    out1.close();

    AppendFileAction appendFileAction = new AppendFileAction();
    appendFileAction.setDfsClient(dfsClient);
    appendFileAction.setContext(smartContext);
    appendFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String,String> args = new HashMap<>();
    args.put(AppendFileAction.FILE_PATH,srcPath+"/"+file1);
    appendFileAction.init(args);
    appendFileAction.run();

    FSDataInputStream inputStream = dfs.open(new Path(srcPath+"/"+file1));

    String readString = "";

    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(inputStream.readByte()==2);
    }

    for (int i = 0; i < 40; i++) {
      Assert.assertTrue(inputStream.readByte()==1);
    }
  }
}
