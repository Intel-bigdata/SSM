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
package org.smartdata.server.actions.mover.defaultmover;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test Mover tool in SSM.
 */
public class TestMover {
  private static final Logger LOG = LoggerFactory.getLogger(TestMover.class);
  private static final int DEFAULT_BLOCK_SIZE = 100;

  static {
    TestBalancer.initTestSetup();
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  static Mover newMover(Configuration conf) throws IOException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Assert.assertEquals(1, namenodes.size());
    Map<URI, List<Path>> nnMap = Maps.newHashMap();
    for (URI nn : namenodes) {
      nnMap.put(nn, null);
    }

    Path moverIdPath = new Path(Mover.MOVER_ID_PATH +
            UUID.randomUUID().toString());
    final List<NameNodeConnector> nncs = NameNodeConnector.newNameNodeConnectors(
            nnMap, Mover.class.getSimpleName(), moverIdPath, conf,
            NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
    return new Mover(nncs.get(0), conf, new AtomicInteger(0), new MoverStatus(null));
  }

  @Test
  public void testScheduleSameBlock() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(4).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testScheduleSameBlock/file";

      {
        final FSDataOutputStream out = dfs.create(new Path(file));
        out.writeChars("testScheduleSameBlock");
        out.close();
      }

      final Mover mover = newMover(conf);
      mover.init();
      final MoverProcessor processor = new MoverProcessor(mover.dispatcher,
          mover.targetPaths, mover.retryCount,
          mover.retryMaxAttempts, mover.storages, new MoverStatus(null));

      final LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      final List<MLocation> locations = MLocation.toLocations(lb);
      final MLocation ml = locations.get(0);
      final Dispatcher.DBlock db = processor.newDBlock(lb, locations);

      final List<StorageType> storageTypes = new ArrayList<StorageType>(
              Arrays.asList(StorageType.DEFAULT, StorageType.DEFAULT));
      Assert.assertTrue(processor.scheduleMoveReplica(db, ml, storageTypes));
      Assert.assertFalse(processor.scheduleMoveReplica(db, ml, storageTypes));
    } finally {
      cluster.shutdown();
    }
  }

  private void testWithinSameNode(Configuration conf) throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(3)
            .storageTypes(
                    new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
            .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testScheduleWithinSameNode/file";
      Path dir = new Path("/testScheduleWithinSameNode");
      dfs.mkdirs(dir);
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out = dfs.create(new Path(file));
      out.writeChars("testScheduleWithinSameNode");
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(dir, "COLD");
      int rc = ToolRunner.run(conf, new MoverCli(),
              new String[] {dir.toString()});
      Assert.assertEquals("Movement to ARCHIVE should be successful", 0, rc);

      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file, 3);
    } finally {
      cluster.shutdown();
    }
  }

  private void testParallelWithinSameNode(Configuration conf) throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(3)
            .storageTypes(
                    new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
            .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file1 = "/testParallelScheduleWithinSameNode/file1";
      final String file2 = "/testParallelScheduleWithinSameNode/file2";
      Path dir = new Path("/testParallelScheduleWithinSameNode");
      dfs.mkdirs(dir);
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out1 = dfs.create(new Path(file1));
      out1.writeChars("testParallelScheduleWithinSameNode1");
      out1.close();
      final FSDataOutputStream out2 = dfs.create(new Path(file2));
      out2.writeChars("testParallelScheduleWithinSameNode2");
      out2.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file1, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      lb = dfs.getClient().getLocatedBlocks(file2, 0).get(0);
      storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(dir, "COLD");
      Thread moverThread1 = new MoverThread(conf, file1);
      Thread moverThread2 = new MoverThread(conf, file2);
      moverThread1.start();
      moverThread2.start();
      while(moverThread1.isAlive() || moverThread2.isAlive()) {
        Thread.sleep(1000);
      }

      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file1, 3);
      waitForLocatedBlockWithArchiveStorageType(dfs, file2, 3);
    } finally {
      cluster.shutdown();
    }
  }

  class MoverThread extends Thread {
    private Configuration conf;
    private String dir;

    public MoverThread(Configuration conf, String dir) {
      this.conf = conf;
      this.dir = dir;
    }

    @Override
    public void run() {
      try {
        int result = ToolRunner.run(conf, new MoverCli(),
                new String[] {dir});
        Assert.assertEquals("Movement to ARCHIVE should be successful", 0, result);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void waitForLocatedBlockWithArchiveStorageType(
          final DistributedFileSystem dfs, final String file,
          final int expectedArchiveCount) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        int archiveCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (StorageType.ARCHIVE == storageType) {
            archiveCount++;
          }
        }
        LOG.info("Archive replica count, expected={} and actual={}",
                expectedArchiveCount, archiveCount);
        return expectedArchiveCount == archiveCount;
      }
    }, 100, 3000);
  }

  @Test
  public void testScheduleBlockWithinSameNode() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    testWithinSameNode(conf);
  }

  @Test
  public void testParallelScheduleBlockWithinSameNode() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    testParallelWithinSameNode(conf);
  }

  private void checkMovePaths(List<Path> actual, Path... expected) {
    Assert.assertEquals(expected.length, actual.size());
    for (Path p : expected) {
      Assert.assertTrue(actual.contains(p));
    }
  }

  /**
   * Test Mover Cli by specifying a list of files/directories using option "-p".
   * There is only one namenode (and hence name service) specified in the conf.
   */
  @Test
  public void testMoverCli() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster
            .Builder(new HdfsConfiguration()).numDataNodes(0).build();
    try {
      final Configuration conf = cluster.getConfiguration(0);
      try {
        MoverCli.getNameNodePathsToMove(conf, "/foo", "bar");
        Assert.fail("Expected exception for illegal path bar");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains("bar is not absolute", e);
      }

      Map<URI, List<Path>> movePaths = MoverCli.getNameNodePathsToMove(conf);
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, namenodes.size());
      Assert.assertEquals(1, movePaths.size());
      URI nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      Assert.assertNull(movePaths.get(nn));

      movePaths = MoverCli.getNameNodePathsToMove(conf, "/foo", "/bar");
      namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, movePaths.size());
      nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      checkMovePaths(movePaths.get(nn), new Path("/foo"), new Path("/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithHAConf() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster
            .Builder(new HdfsConfiguration())
            .nnTopology(MiniDFSNNTopology.simpleHATopology())
            .numDataNodes(0).build();
    HATestUtil.setFailoverConfigurations(cluster, conf, "MyCluster");
    try {
      Map<URI, List<Path>> movePaths = MoverCli.getNameNodePathsToMove(conf,
             "/foo", "/bar");
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, namenodes.size());
      Assert.assertEquals(1, movePaths.size());
      URI nn = namenodes.iterator().next();
      Assert.assertEquals(new URI("hdfs://MyCluster"), nn);
      Assert.assertTrue(movePaths.containsKey(nn));
      checkMovePaths(movePaths.get(nn), new Path("/foo"), new Path("/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithFederation() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster
            .Builder(new HdfsConfiguration())
            .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(3))
            .numDataNodes(0).build();
    final Configuration conf = new HdfsConfiguration();
    DFSTestUtil.setFederatedConfiguration(cluster, conf);
    try {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(3, namenodes.size());

      try {
        MoverCli.getNameNodePathsToMove(conf, "/foo");
        Assert.fail("Expect exception for missing authority information");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains(
                "does not contain scheme and authority", e);
      }

      try {
        MoverCli.getNameNodePathsToMove(conf, "hdfs:///foo");
        Assert.fail("Expect exception for missing authority information");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains(
                "does not contain scheme and authority", e);
      }

      try {
        MoverCli.getNameNodePathsToMove(conf, "wrong-hdfs://ns1/foo");
        Assert.fail("Expect exception for wrong scheme");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains("Cannot resolve the path", e);
      }

      Iterator<URI> iter = namenodes.iterator();
      URI nn1 = iter.next();
      URI nn2 = iter.next();
      Map<URI, List<Path>> movePaths = MoverCli.getNameNodePathsToMove(conf,
              nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar");
      Assert.assertEquals(2, movePaths.size());
      checkMovePaths(movePaths.get(nn1), new Path("/foo"), new Path("/bar"));
      checkMovePaths(movePaths.get(nn2), new Path("/foo/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithFederationHA() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster
            .Builder(new HdfsConfiguration())
            .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(3))
            .numDataNodes(0).build();
    final Configuration conf = new HdfsConfiguration();
    DFSTestUtil.setFederatedHAConfiguration(cluster, conf);
    try {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(3, namenodes.size());

      Iterator<URI> iter = namenodes.iterator();
      URI nn1 = iter.next();
      URI nn2 = iter.next();
      URI nn3 = iter.next();
      Map<URI, List<Path>> movePaths = MoverCli.getNameNodePathsToMove(conf,
              nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar", nn3 + "/foobar");
      Assert.assertEquals(3, movePaths.size());
      checkMovePaths(movePaths.get(nn1), new Path("/foo"), new Path("/bar"));
      checkMovePaths(movePaths.get(nn2), new Path("/foo/bar"));
      checkMovePaths(movePaths.get(nn3), new Path("/foobar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testTwoReplicaSameStorageTypeShouldNotSelect() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(3)
            .storageTypes(
                    new StorageType[][] { { StorageType.DISK, StorageType.ARCHIVE },
                            { StorageType.DISK, StorageType.DISK },
                            { StorageType.DISK, StorageType.ARCHIVE } }).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testForTwoReplicaSameStorageTypeShouldNotSelect";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testForTwoReplicaSameStorageTypeShouldNotSelect");
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new MoverCli(),
              new String[] {file.toString() });
      Assert.assertEquals("Movement to ARCHIVE should be successful", 0, rc);

      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file, 2);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverFailedRetry() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.set(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY, "2");
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(3)
            .storageTypes(
                    new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE},
                            {StorageType.DISK, StorageType.ARCHIVE},
                            {StorageType.DISK, StorageType.ARCHIVE}}).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoverFailedRetry";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testMoverFailedRetry");
      out.close();

      // Delete block file so, block move will fail with FileNotFoundException
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      cluster.corruptBlockOnDataNodesByDeletingBlockFile(lb.getBlock());
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new MoverCli(),
              new String[] {file.toString()});
      Assert.assertEquals("Movement should fail after some retry",
              ExitStatus.NO_MOVE_PROGRESS.getExitCode(), rc);
    } finally {
      cluster.shutdown();
    }
  }
}
