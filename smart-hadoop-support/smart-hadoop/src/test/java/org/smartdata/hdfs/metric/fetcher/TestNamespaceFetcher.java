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
package org.smartdata.hdfs.metric.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.smartdata.model.FileInfo;
import org.smartdata.conf.SmartConf;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestNamespaceFetcher {
  @Test
  public void testNamespaceFetcher() throws IOException, InterruptedException,
      MissingEventsException, MetaStoreException {
    final Configuration conf = new SmartConf();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(2).build();
    try {
      final DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdir(new Path("/user"), new FsPermission("777"));
      dfs.create(new Path("/user/user1"));
      dfs.create(new Path("/user/user2"));
      dfs.mkdir(new Path("/tmp"), new FsPermission("777"));
      DFSClient client = dfs.getClient();

      MetaStore adapter = Mockito.mock(MetaStore.class);
      final List<String> pathesInDB = new ArrayList<>();
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocationOnMock) {
          try {
            Object[] objects = invocationOnMock.getArguments();
            for (FileInfo fileInfo : (FileInfo[]) objects[0]) {
              pathesInDB.add(fileInfo.getPath());
            }
          } catch (Throwable t) {
            t.printStackTrace();
          }
          return null;
        }
      }).when(adapter).insertFiles(any(FileInfo[].class));
      NamespaceFetcher fetcher = new NamespaceFetcher(client, adapter, 100);
      fetcher.startFetch();
      List<String> expected = Arrays.asList("/", "/user", "/user/user1", "/user/user2", "/tmp");
      while (!fetcher.fetchFinished()) {
        Thread.sleep(100);
      }
      Assert.assertTrue(pathesInDB.size() == expected.size() && pathesInDB.containsAll(expected));
      fetcher.stop();
    } finally {
      cluster.shutdown();
    }
  }
}
