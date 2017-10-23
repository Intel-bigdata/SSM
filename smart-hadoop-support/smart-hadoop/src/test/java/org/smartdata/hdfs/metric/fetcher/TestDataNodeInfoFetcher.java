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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metastore.TestDaoUtil;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class TestDataNodeInfoFetcher extends TestDaoUtil {

  private MetaStore metaStore;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem dfs;
  protected DFSClient dfsClient;
  private Configuration conf;
  ScheduledExecutorService scheduledExecutorService;
  DataNodeInfoFetcher fetcher;

  @Before
  public void init() throws Exception {
    initDao();
    conf = new SmartConf();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = dfs.getClient();
    scheduledExecutorService = Executors.newScheduledThreadPool(2);
    metaStore = new MetaStore(druidPool);
    fetcher = new DataNodeInfoFetcher(dfsClient, metaStore,
        scheduledExecutorService, conf);
  }

  @After
  public void shutdown() throws Exception {
    fetcher.stop();
    fetcher = null;
    closeDao();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDataNodeInfoFetcher() throws IOException, InterruptedException,
      MissingEventsException, MetaStoreException {
    fetcher.start();
    while (!fetcher.isFetchFinished()) {
      Thread.sleep(1000);
    }
    Assert.assertEquals(2,metaStore.getAllDataNodeInfo().size());
  }
}
