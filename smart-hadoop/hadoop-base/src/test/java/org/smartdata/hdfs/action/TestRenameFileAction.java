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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for RenameFileAction.
 */
public class TestRenameFileAction extends ActionMiniCluster {

  @Test
  public void testLocalFileRename() throws IOException {
    final String srcPath = "/testRename";
    final String file1 = "file1";
    final String destPath = "/destDir";
    final String destFilename = "file2";
    Path srcDir = new Path(srcPath);
    dfs.mkdirs(srcDir);
    //the parent dir need to be exist
    dfs.mkdirs(new Path(destPath));
    //write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    out1.writeChars("testCopy1");
    out1.close();
    Assert.assertTrue(dfsClient.exists(srcPath + "/" + file1));

    RenameFileAction renameFileAction = new RenameFileAction();
    renameFileAction.setDfsClient(dfsClient);
    renameFileAction.setContext(smartContext);
    renameFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(RenameFileAction.FILE_PATH, srcPath + "/" + file1);
    args.put(RenameFileAction.DEST_PATH, destPath + "/" + destFilename);
    renameFileAction.init(args);
    renameFileAction.run();
    Assert.assertFalse(dfsClient.exists(srcPath + "/" + file1));
    Assert.assertTrue(dfsClient.exists(destPath + "/" + destFilename));
  }

  @Test
  public void testRemoteFileRename() throws IOException {
    final String srcPath = "/testRename";
    final String file1 = "file1";
    final String destPath = "/destDir";
    final String destFilename = "file2";

    dfs.mkdirs(new Path(dfs.getUri() + srcPath));
    dfs.mkdirs(new Path(dfs.getUri() + destPath));

    // write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path( dfs.getUri() + srcPath + "/" + file1));
    out1.writeChars("testCopy1");
    out1.close();

    RenameFileAction renameFileAction = new RenameFileAction();
    renameFileAction.setDfsClient(dfsClient);
    renameFileAction.setContext(smartContext);
    renameFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String , String> args = new HashMap<>();
    args.put(RenameFileAction.FILE_PATH , dfs.getUri() + srcPath + "/" +file1);
    args.put(RenameFileAction.DEST_PATH , dfs.getUri() + destPath + "/" +destFilename);
    renameFileAction.init(args);
    renameFileAction.run();
    Assert.assertFalse(dfsClient.exists(srcPath + "/" + file1));
    Assert.assertTrue(dfsClient.exists(destPath + "/" + destFilename));
  }
}
