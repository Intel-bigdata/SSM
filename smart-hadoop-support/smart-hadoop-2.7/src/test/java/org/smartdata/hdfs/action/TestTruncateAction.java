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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class TestTruncateAction extends MiniClusterHarness {
  @Test
  public void testLocalTruncateFile() throws IOException, InterruptedException {
    final String srcPath = "/test";
    final String file = "file";

    dfs.mkdirs(new Path(srcPath));
    FSDataOutputStream out = dfs.create(new Path(srcPath + "/" + file));

    for (int i = 0; i < 50; i++) {
      out.writeByte(1);
    }

    out.close();

    TruncateAction truncateAction = new TruncateAction();
    truncateAction.setDfsClient(dfsClient);
    truncateAction.setContext(smartContext);
    truncateAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(TruncateAction.FILE_PATH, srcPath + "/" + file);
    args.put(TruncateAction.LENGTH, "20");

    truncateAction.init(args);
    truncateAction.run();


    FSDataInputStream in = dfs.open(new Path(srcPath + "/" + file),50);
    //check the length
    Thread.sleep(10000);
    long newLength = dfs.getFileStatus(new Path(srcPath + "/" + file)).getLen();

    Assert.assertTrue(newLength == 20);
  }

  @Test
  public void testRemoteTruncateFile() throws IOException, InterruptedException {
    final String srcPath = "/test";
    final String file = "file";

    dfs.mkdirs(new Path(srcPath));
    FSDataOutputStream out = dfs.create(new Path(srcPath + "/" + file));

    for (int i = 0; i < 50; i++) {
      out.writeByte(1);
    }

    out.close();

    TruncateAction truncateAction = new TruncateAction();
    truncateAction.setDfsClient(dfsClient);
    truncateAction.setContext(smartContext);
    truncateAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(TruncateAction.FILE_PATH, dfs.getUri() + srcPath + "/" + file);
    args.put(TruncateAction.LENGTH, "20");

    truncateAction.init(args);
    truncateAction.run();


    FSDataInputStream in = dfs.open(new Path(srcPath + "/" + file),50);
    //check the length
    Thread.sleep(10000);
    long newLength = dfs.getFileStatus(new Path(srcPath + "/" + file)).getLen();

    Assert.assertTrue(newLength == 20);
  }
}
