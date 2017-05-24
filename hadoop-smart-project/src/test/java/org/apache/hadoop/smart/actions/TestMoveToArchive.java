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
package org.apache.hadoop.smart.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.smart.mover.MoverPool;
import org.apache.hadoop.smart.mover.Status;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Move to Archive Unit Test
 */
public class TestMoveToArchive {
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final String REPLICATION_KEY = "3";

  private void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY,REPLICATION_KEY);
  }
  @Test
  public void MoveToArchive() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    MoverPool.getInstance().init(conf);
    // Move File from SSD to Archive
    testMoveFileToArchive(conf);
  }


  private void testMoveFileToArchive(Configuration conf) throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
            numDataNodes(3).
            storageTypes(new StorageType[]
                    {StorageType.DISK,StorageType.ARCHIVE}).
            build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveFileToArchive/file";
      Path dir = new Path("/testMoveFileToArchive");
      final DFSClient client = cluster.getFileSystem().getClient();
      dfs.mkdirs(dir);
      String[] args = {file};
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out = dfs.create(new Path(file),true,1024);
      out.writeChars(file);
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to Archive, Policy CLOD
      UUID id = new MoveFile(client, conf, "COLD").initial(args).execute();
      Status status = MoverPool.getInstance().getStatus(id);
      while (!status.getIsFinished()) {
        Thread.sleep(3000);
      }

      // verify after movement
      Assert.assertTrue(status.getSucceeded());
      LocatedBlock lb1 = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes1 = lb1.getStorageTypes();
      for (StorageType storageType : storageTypes1) {
        Assert.assertTrue(StorageType.ARCHIVE == storageType);
      }

    } finally {
      cluster.shutdown();
    }
  }
}