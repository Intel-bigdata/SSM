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
 * Test for DeleteFileAction.
 */
public class TestDeleteFileAction extends ActionMiniCluster{

  @Test
  public void testLocalFileDelete() throws IOException {
    final String srcPath = "/testDel";
    final String file1 = "file1";
    Path srcDir = new Path(srcPath);
    dfs.mkdirs(srcDir);

    //write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    out1.writeChars("testDelete1");
    out1.close();
    Assert.assertTrue(dfsClient.exists(srcPath + "/" + file1));

    DeleteFileAction deleteFileAction = new DeleteFileAction();
    deleteFileAction.setDfsClient(dfsClient);
    deleteFileAction.setContext(smartContext);
    deleteFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(DeleteFileAction.FILE_PATH, srcPath + "/" + file1);
    deleteFileAction.init(args);
    deleteFileAction.run();
    //Check if file is not exist
    Assert.assertFalse(dfsClient.exists(srcPath + "/" + file1));
  }

  @Test
  public void testRemoteFileDelete() throws Exception {
    final String srcPath = "/testDel";
    final String file1 = "file1";
    Path srcDir = new Path(srcPath);
    dfs.mkdirs(srcDir);
    //Write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    out1.writeChars("testDelete1");
    out1.close();
    Assert.assertTrue(dfsClient.exists(srcPath + "/" + file1));

    DeleteFileAction deleteFileAction = new DeleteFileAction();
    deleteFileAction.setDfsClient(dfsClient);
    deleteFileAction.setContext(smartContext);
    deleteFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    // Destination with "hdfs" prefix
    args.put(DeleteFileAction.FILE_PATH, dfs.getUri() + srcPath + "/" + file1);
    deleteFileAction.init(args);
    deleteFileAction.run();
    Assert.assertFalse(dfsClient.exists(srcPath + "/" + file1));
  }
}
