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

public class TestTruncate0 extends MiniClusterHarness {
  @Test
  public void testSetFileLen() throws IOException, InterruptedException {
    final String srcPath = "/test";
    final String file = "file";

    dfs.mkdirs(new Path(srcPath));
    FSDataOutputStream out = dfs.create(new Path(srcPath + "/" + file));

    for (int i = 0; i < 50; i++) {
      out.writeByte(1);
    }

    out.close();

    Truncate0Action setLen2ZeroAction = new Truncate0Action();
    setLen2ZeroAction.setDfsClient(dfsClient);
    setLen2ZeroAction.setContext(smartContext);
    setLen2ZeroAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(Truncate0Action.FILE_PATH, srcPath + "/" + file);

    setLen2ZeroAction.init(args);
    setLen2ZeroAction.run();

    long newLength = dfs.getFileStatus(new Path(srcPath + "/" + file)).getLen();

    System.out.println(newLength);
    Assert.assertTrue(newLength == 0);
  }
}
