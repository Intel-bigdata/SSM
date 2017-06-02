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
package org.smartdata.server.command;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.actions.hdfs.CacheFileAction;
import org.smartdata.actions.hdfs.HdfsAction;
import org.smartdata.actions.hdfs.MoveFileAction;
import org.smartdata.common.CommandState;
import org.smartdata.conf.SmartConf;

import java.io.IOException;
import java.util.UUID;


/**
 * CommandPool Unit Test
 */
public class TestCommandPool {
  private static final int DEFAULT_BLOCK_SIZE = 50;
  private MiniDFSCluster cluster;
  private DFSClient client;
  private DistributedFileSystem dfs;
  private SmartConf smartConf = new SmartConf();

  @Before
  public void createCluster() throws IOException {
    smartConf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    smartConf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    smartConf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    smartConf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    smartConf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
    cluster = new MiniDFSCluster.Builder(smartConf)
        .numDataNodes(3)
        .storagesPerDatanode(3)
        .storageTypes(new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
            StorageType.SSD})
        .build();
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
  public void addCommand() throws Exception {
    generateTestCase();
    Command cmd = runHelper();
    CommandPool cmdPool = new CommandPool();
    cmdPool.execute(cmd);
    cmdPool.getCommandThread(cmd.getId()).join();
  }

  /**
   * Delete a command
   */
  @Test
  public void deleteCommand() throws Exception {
    generateTestCase();
    Command cmd = runHelper();
    CommandPool cmdPool = new CommandPool();
    cmdPool.execute(cmd);
    Thread.sleep(1000);
    cmdPool.deleteCommand(cmd.getId());
  }

  private void generateTestCase() throws IOException {
    final DistributedFileSystem dfs = cluster.getFileSystem();
    // New dir
    Path dir = new Path("/testMoveFile");
    dfs.mkdirs(dir);
    // Move to SSD
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out1 = dfs.create(new Path("/testMoveFile/file1"),
        true, 1024);
    out1.writeChars("/testMoveFile/file1");
    out1.close();
    // Move to Archive
    final FSDataOutputStream out2 = dfs.create(new Path("/testMoveFile/file2"),
        true, 1024);
    out2.writeChars("/testMoveFile/file2");
    out2.close();
    // Move to Cache
    Path dir3 = new Path("/testCacheFile");
    dfs.mkdirs(dir3);
  }

  private Command runHelper() throws IOException {
    HdfsAction[] actions = new HdfsAction[4];
    // New action
    // actions[0] = new MoveFileAction();
    // actions[0].setDfsClient(client);
    // actions[0].setContext(new SmartContext(smartConf));
    // actions[0].init(new String[]{"/testMoveFile/file1", "ALL_SSD"});
    // actions[0].getActionStatus().setId(UUID.randomUUID());
    // actions[1] = new MoveFileAction();
    // actions[1].setDfsClient(client);
    // actions[1].setContext(new SmartContext(smartConf));
    // actions[1].init(new String[]{"/testMoveFile/file2", "COLD"});
    // actions[1].getActionStatus().setId(UUID.randomUUID());
    actions[2] = new CacheFileAction();
    actions[2].setDfsClient(client);
    actions[2].setContext(new SmartContext(smartConf));
    actions[2].init(new String[]{"/testCacheFile"});
    actions[2].getActionStatus().setId(UUID.randomUUID());
    // New Command
    Command cmd = new Command(actions, null);
    cmd.setId(1);
    cmd.setRuleId(1);
    cmd.setState(CommandState.PENDING);
    return cmd;
  }
}
