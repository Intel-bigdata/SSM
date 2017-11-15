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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.hdfs.MiniClusterHarness;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for CopyFileAction.
 */
public class TestCopyFileAction extends MiniClusterHarness {

  private void copyFile(String src, String dest, long length,
      long offset) throws Exception {
    CopyFileAction copyFileAction = new CopyFileAction();
    copyFileAction.setDfsClient(dfsClient);
    copyFileAction.setContext(smartContext);
    copyFileAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(CopyFileAction.FILE_PATH, src);
    args.put(CopyFileAction.DEST_PATH, dest);
    args.put(CopyFileAction.LENGTH, "" + length);
    args.put(CopyFileAction.OFFSET_INDEX, "" + offset);
    copyFileAction.init(args);
    copyFileAction.run();
  }

  /*@Test
  public void testLocalFileCopy() throws Exception {
    final String srcPath = "/testCopy";
    final String file1 = "file1";
    final String destPath = "/backup";
    Path srcDir = new Path(srcPath);
    dfs.mkdirs(srcDir);
    dfs.mkdirs(new Path(destPath));
    // write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    out1.writeChars("testCopy1");
    out1.close();
    copyFile(srcPath + "/" + file1, destPath + "/" + file1, 0, 0);
    // Check if file exists
    Assert.assertTrue(dfsClient.exists(destPath + "/" + file1));
    final FSDataInputStream in1 = dfs.open(new Path(destPath + "/" + file1));
    StringBuilder readString = new StringBuilder();
    for (int i = 0; i < 9; i++) {
      readString.append(in1.readChar());
    }
    Assert.assertTrue(readString.toString().equals("testCopy1"));
  }*/

  @Test
  public void testRemoteFileCopy() throws Exception {
    final String srcPath = "/testCopy";
    final String file1 = "file1";
    // Destination with "hdfs" prefix
    final String destPath = dfs.getUri() + "/backup";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    out1.writeChars("testCopy1");
    out1.close();

    copyFile(srcPath + "/" + file1, destPath + "/" + file1, 0, 0);
    // Check if file exists
    Assert.assertTrue(dfsClient.exists("/backup/" + file1));
    final FSDataInputStream in1 = dfs.open(new Path(destPath + "/" + file1));
    StringBuilder readString = new StringBuilder();
    for (int i = 0; i < 9; i++) {
      readString.append(in1.readChar());
    }
    Assert.assertTrue(readString.toString().equals("testCopy1"));
  }

  @Test
  public void testRmoteCopyWithOffset() throws Exception {
    final String srcPath = "/testCopy";
    final String file1 = "file1";
    // Destination with "hdfs" prefix
    final String destPath = dfs.getUri() + "/backup";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    for (int i = 0; i < 50; i++) {
      out1.writeByte(1);
    }
    for (int i = 0; i < 50; i++) {
      out1.writeByte(2);
    }
    out1.close();
    copyFile(srcPath + "/" + file1, destPath + "/" + file1, 50, 50);
    // Check if file exists
    Assert.assertTrue(dfsClient.exists("/backup/" + file1));
    final FSDataInputStream in1 = dfs.open(new Path(destPath + "/" + file1));
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(in1.readByte() == 2);
    }
  }

  /*@Test
  public void testLocalCopyWithOffset() throws Exception {
    final String srcPath = "/testCopy";
    final String file1 = "file1";
    // Destination with "hdfs" prefix
    final String destPath = "/backup";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // write to DISK
    final FSDataOutputStream out1 = dfs.create(new Path(srcPath + "/" + file1));
    for (int i = 0; i < 50; i++) {
      out1.writeByte(1);
    }
    for (int i = 0; i < 50; i++) {
      out1.writeByte(2);
    }
    out1.close();
    copyFile(srcPath + "/" + file1, destPath + "/" + file1, 50, 50);
    // Check if file exists
    Assert.assertTrue(dfsClient.exists("/backup/" + file1));
    final FSDataInputStream in1 = dfs.open(new Path(destPath + "/" + file1));
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(in1.readByte() == 2);
    }
  }*/

  @Test
  public void testAppendRemote() throws Exception {
    final String srcPath = "/testCopy";
    final String file1 = "file1";
    // Destination with "hdfs" prefix
    final String destPath = dfs.getUri() + "/backup";
    dfs.mkdirs(new Path(srcPath));
    dfs.mkdirs(new Path(destPath));
    // write to DISK
    DFSTestUtil.createFile(dfs, new Path(srcPath + "/" + file1), 100, (short) 3,
        0xFEED);
    DFSTestUtil.createFile(dfs, new Path(destPath + "/" + file1), 50, (short) 3,
        0xFEED);
    copyFile(srcPath + "/" + file1, destPath + "/" + file1, 50, 50);
    // Check if file exists
    Assert.assertTrue(dfsClient.exists("/backup/" + file1));
    FileStatus fileStatus = dfs.getFileStatus(new Path(destPath + "/" + file1));
    Assert.assertEquals(100, fileStatus.getLen());
  }
}
