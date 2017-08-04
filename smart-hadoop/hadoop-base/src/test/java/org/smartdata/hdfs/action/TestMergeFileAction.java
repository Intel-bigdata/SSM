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
import org.smartdata.actions.MockActionStatusReporter;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for MergeFileAction
 */
public class TestMergeFileAction extends ActionMiniCluster {

  @Test
  public void testLocalFileMerge() throws Exception {
    final String srcPath = "/testMerge";
    final String file1 = "file1";
    final String file2 = "file2";
    final String target = "/target";

    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(target));
    //write to DISK
    //write 40 Bytes to file1 and 50 Byte to file2. then concat them
    FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    for (int i = 0; i < 60; i++) {
      out1.writeByte(1);
    }
    out1.close();

    out1 = dfs.create(new Path(srcPath + "/" + file2));
    for (int i = 0; i < 50; i++) {
      out1.writeByte(2);
    }
    out1.close();

    MergeFileAction mergeFileAction = new MergeFileAction();
    mergeFileAction.setDfsClient(dfsClient);
    mergeFileAction.setContext(smartContext);
    mergeFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(MergeFileAction.FILE_PATH, srcPath + "/" +
        file1 + "," + dfs.getUri() + srcPath + "/" + "file2");
    args.put(MergeFileAction.DEST_PATH, target);
    mergeFileAction.init(args);
    mergeFileAction.run();

    Assert.assertTrue(dfsClient.exists(target));
    //read and check file
    FSDataInputStream in = dfs.open(new Path(target),50);
    for (int i = 0; i < 60; i++) {
      Assert.assertTrue(in.readByte() == 1);
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(in.readByte() == 2);
    }
  }

  @Test
  public void testRemoteFileMerge() throws Exception {
    final String srcPath = "/testMerge";
    final String file1 = "file1";
    final String file2 = "file2";
    final String target = "/target";

    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(target));
    //write to DISK
    //write 40 Bytes to file1 and 50 Byte to file2. then concat them
    FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    for (int i = 0; i < 60; i++) {
      out1.writeByte(1);
    }
    out1.close();

    out1 = dfs.create(new Path(srcPath + "/" + file2));
    for (int i = 0; i < 50; i++) {
      out1.writeByte(2);
    }
    out1.close();

    MergeFileAction mergeFileAction = new MergeFileAction();
    mergeFileAction.setDfsClient(dfsClient);
    mergeFileAction.setContext(smartContext);
    mergeFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(MergeFileAction.FILE_PATH, dfs.getUri() + srcPath + "/" +
        file1 + "," + dfs.getUri() + srcPath + "/" + "file2");
    args.put(MergeFileAction.DEST_PATH, dfs.getUri() + target);
    mergeFileAction.init(args);
    mergeFileAction.run();

    Assert.assertTrue(dfsClient.exists(target));
    //read and check file
    FSDataInputStream in = dfs.open(new Path(target),50);
    for (int i = 0; i < 60; i++) {
      Assert.assertTrue(in.readByte() == 1);
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(in.readByte() == 2);
    }
  }
}
