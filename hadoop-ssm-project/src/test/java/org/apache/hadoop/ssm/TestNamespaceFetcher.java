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
package org.apache.hadoop.ssm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.FileStatusInternal;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestNamespaceFetcher {

  public class FileStatusArgMatcher extends ArgumentMatcher<FileStatusInternal[]> {
    private List<String> expected;

    public FileStatusArgMatcher(List<String> path) {
      this.expected = path;
    }

    @Override
    public boolean matches(Object o) {
      FileStatusInternal[] array = (FileStatusInternal[]) o;
      List<String> paths = Arrays.stream(array).map(FileStatusInternal::getPath)
        .collect(Collectors.toList());
      Collections.sort(paths);
      return paths.size() == expected.size() && paths.containsAll(expected);
    }
  }

  @Test
  public void testNamespaceFetcher() throws IOException, InterruptedException {
    final Configuration conf = new SSMConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(2).build();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    dfs.mkdir(new Path("/user"), new FsPermission("777"));
    dfs.create(new Path("/user/user1"));
    dfs.create(new Path("/user/user2"));
    dfs.mkdir(new Path("/tmp"), new FsPermission("777"));
    DFSClient client = dfs.getClient();

    DBAdapter adapter = mock(DBAdapter.class);
    NamespaceFetcher fetcher = new NamespaceFetcher(client, adapter, 100);
    fetcher.startFetch();
    List<String> expected = Arrays.asList("/", "/user", "/user/user1", "/user/user2", "/tmp");
    Thread.sleep(1000);

    verify(adapter).insertFiles(argThat(new FileStatusArgMatcher(expected)));
    fetcher.stop();
  }
}
