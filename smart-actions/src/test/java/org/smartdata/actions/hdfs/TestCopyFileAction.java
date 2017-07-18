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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.MockActionStatusReporter;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for CopyFileAction.
 */
public class TestCopyFileAction extends ActionMiniCluster {

  @Test
  public void testLocalFileCopy() throws Exception {
    final String srcPath = "/testCopy";
    final String file1 = "file1";
    final String distPath = "/backup";
    Path srcDir = new Path(srcPath);
    dfs.mkdirs(srcDir);
    dfs.mkdirs(new Path(distPath));
    // write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    out1.writeChars("testCopy1");
    out1.close();

    CopyFileAction copyFileAction = new CopyFileAction();
    copyFileAction.setDfsClient(dfsClient);
    copyFileAction.setContext(smartContext);
    copyFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(CopyFileAction.FILE_PATH, srcPath + "/" + file1);
    args.put(CopyFileAction.REMOTE_URL, distPath + "/" + file1);
    copyFileAction.init(args);
    copyFileAction.run();
    // Check if file exists
    Assert.assertTrue(dfsClient.exists(distPath + "/" + file1));
  }

  @Test
  public void testDirCopy() throws Exception {

  }

}
