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
package org.smartdata.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.model.FileDiff;
import org.smartdata.server.engine.CopyScheduler;
import org.smartdata.server.engine.CopyTargetTask;

import java.io.IOException;
import java.util.List;

public class TestCopySchedular extends TestEmptyMiniSmartCluster{

  private static final int DEFAULT_BLOCK_SIZE = 50;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem dfs;
  protected DFSClient dfsClient;
  protected SmartContext smartContext;

  static {
    TestBalancer.initTestSetup();
  }

  @Before
  public void init() throws Exception {
    SmartConf conf = new SmartConf();
    initConf(conf);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(5)
        .storagesPerDatanode(3)
        .storageTypes(new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
            StorageType.SSD})
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = dfs.getClient();
    smartContext = new SmartContext(conf);
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  @Test
  public void testFileDiffParsing() throws Exception {
    FileDiff fileDiff = new FileDiff();
    fileDiff.setParameters("-file /root/test -dest /root/test2");
    String cmd = CopyScheduler.cmdParsing(fileDiff, "/root/", "/lcoalhost:3306/backup/");
    Assert.assertTrue(cmd.equals("Copy -file /root/test -dest /lcoalhost:3306/backup/test2"));
    fileDiff.setParameters("-file /root/test -dest /root/test2 -length 1024");
    cmd = CopyScheduler.cmdParsing(fileDiff, "/root/", "/lcoalhost:3306/backup/");
    Assert.assertTrue(cmd.equals("Copy -file /root/test -dest /lcoalhost:3306/backup/test2 -length 1024"));
  }

  @Test
  public void testCopyScheduler() throws IOException {
    final String srcPath = "/testCopy";
    final String file1 = "file1";
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
    for (int i = 0; i < 50; i++) {
      out1.writeByte(3);
    }
    out1.close();

    // System.out.println(dfs.getFileStatus(new Path(srcPath + "/" + file1)).getLen());
    List<CopyTargetTask> copyTargetTaskList = CopyScheduler.splitCopyFile(srcPath + "/" + file1,
        destPath + "/" + file1, 1, FileSystem.get(dfs.getUri(), new Configuration()));

    System.out.println(copyTargetTaskList);

    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(copyTargetTaskList.get(i).getDest().equals("/backup/file1_temp_chunkCount" + (i + 1)));
    }
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
