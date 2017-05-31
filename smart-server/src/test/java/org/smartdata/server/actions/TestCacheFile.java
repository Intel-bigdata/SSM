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
package org.smartdata.server.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


/**
 * Move to Cache Unit Test
 */
public class TestCacheFile {

  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";
  private MiniDFSCluster cluster;
  private DFSClient client;
  private DistributedFileSystem dfs;
  private Configuration conf = new Configuration();

  @Before
  public void newCluster()  throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION_KEY);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(3).
        storageTypes(new StorageType[]
            {StorageType.DISK, StorageType.ARCHIVE}).
        build();
    client = cluster.getFileSystem().getClient();
    dfs = cluster.getFileSystem();
    cluster.waitActive();
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testMkdir() throws IOException {
    Path dir = new Path("/fileTestA");
    dfs.mkdirs(dir);
    String[] str = {"/fileTestA"};
    CacheFile moveToCache = new CacheFile();
    moveToCache.initial(client, conf, str);
    assertEquals(false, moveToCache.isCached(str[0]));
    moveToCache.run();
    assertEquals(true, moveToCache.isCached(str[0]));
  }
}