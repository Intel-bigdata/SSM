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
package org.apache.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FilesInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDFSGetFilesInfo {
  @Test(timeout=30000)
  public void testGetFilesInfo() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    String[] filePaths = new String[]{"/dir/a/af1", "/dir/a/af2",
        "/dir/a/d1/df1", "/dir/a/d1/df2"};
    for (String f : filePaths) {
      DFSTestUtil.createFile(fs, new Path(f), 1024, (short) 3, 0);
    }

    String[] dirs = new String[]{"/dir/a"};
    FilesInfo info = fs.getClient()
        .getFilesInfo(dirs, FilesInfo.ALL, true, false);
    List<String> allPaths = info.getAllPaths();
    assertEquals(allPaths.size(), filePaths.length);
    for (int i = 0; i < allPaths.size(); i++) {
      assertEquals("Expect [" + filePaths[i] + "] while get ["
          + allPaths.get(i) + "].", filePaths[i], allPaths.get(i));
    }
  }
}
