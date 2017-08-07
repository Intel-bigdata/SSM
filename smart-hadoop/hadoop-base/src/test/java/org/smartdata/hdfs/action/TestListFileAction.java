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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.MockActionStatusReporter;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for ListFileAction.
 */
public class TestListFileAction extends ActionMiniCluster {
  @Test
  public void testRemoteFileList() throws Exception {
    final String srcPath = "/testList";
    final String file1 = "file1";
    final String file2 = "file2";
    final String dir = "/childDir";
    final String dirFile1 = "childDirFile1";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(srcPath + dir));
    //write to DISK
    FSDataOutputStream out1 = dfs.create(new Path(dfs.getUri() + srcPath + "/" + file1));
    out1.writeChars("test");
    out1.close();
    out1 = dfs.create(new Path(dfs.getUri() + srcPath + "/" + file2));
    out1.writeChars("test");
    out1.close();
    out1 = dfs.create(new Path(dfs.getUri() + srcPath + dir + "/" + dirFile1));
    out1.writeChars("test");
    out1.close();

    Assert.assertTrue(dfsClient.exists(srcPath + "/" + file1));
    Assert.assertTrue(dfsClient.exists(srcPath + "/" + file2));
    Assert.assertTrue(dfsClient.exists(srcPath + dir + "/" + dirFile1));

    ListFileAction listFileAction = new ListFileAction();
    listFileAction.setDfsClient(dfsClient);
    listFileAction.setContext(smartContext);
    listFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String , String> args = new HashMap<>();
    args.put(ListFileAction.FILE_PATH , dfs.getUri() + srcPath);
    listFileAction.init(args);
    listFileAction.run();

    listFileAction = new ListFileAction();
    listFileAction.setDfsClient(dfsClient);
    listFileAction.setContext(smartContext);
    listFileAction.setStatusReporter(new MockActionStatusReporter());
    args = new HashMap<>();
    args.put(ListFileAction.FILE_PATH , dfs.getUri() + srcPath + "/" + file1);
    listFileAction.init(args);
    listFileAction.run();
  }

  @Test
  public void testLocalFileList() throws Exception {
    final String srcPath = "/testList";
    final String file1 = "file1";
    final String file2 = "file2";
    final String dir = "/childDir";
    final String dirFile1 = "childDirFile1";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(srcPath + dir));
    //write to DISK
    FSDataOutputStream out1 = dfs.create(new Path(dfs.getUri() + srcPath + "/" + file1));
    out1.writeChars("test");
    out1.close();
    out1 = dfs.create(new Path(srcPath + "/" + file2));
    out1.writeChars("test");
    out1.close();
    out1 = dfs.create(new Path(srcPath + dir + "/" + dirFile1));
    out1.writeChars("test");
    out1.close();

    Assert.assertTrue(dfsClient.exists(srcPath + "/" + file1));
    Assert.assertTrue(dfsClient.exists(srcPath + "/" + file2));
    Assert.assertTrue(dfsClient.exists(srcPath + dir + "/" + dirFile1));

    ListFileAction listFileAction = new ListFileAction();
    listFileAction.setDfsClient(dfsClient);
    listFileAction.setContext(smartContext);
    listFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String , String> args = new HashMap<>();
    args.put(ListFileAction.FILE_PATH , srcPath);
    listFileAction.init(args);
    listFileAction.run();

    listFileAction = new ListFileAction();
    listFileAction.setDfsClient(dfsClient);
    listFileAction.setContext(smartContext);
    listFileAction.setStatusReporter(new MockActionStatusReporter());
    args = new HashMap<>();
    args.put(ListFileAction.FILE_PATH , srcPath + "/" + file1);
    listFileAction.init(args);
    listFileAction.run();
  }
}
