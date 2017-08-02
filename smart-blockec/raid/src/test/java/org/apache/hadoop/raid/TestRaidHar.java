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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * If a file gets deleted, then verify that the parity file gets deleted too.
 */
public class TestRaidHar extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidNode");
  final Random rand = new Random();
  final static int NUM_DATANODES = 3;

  {
    ((Log4JLogger)RaidNode.LOG).getLogger().setLevel(Level.ALL);
  }


  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;

  /**
   * create mapreduce and dfs clusters
   */
  private void createClusters(boolean local) throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);
    conf.set("mapred.raid.http.address", "localhost:0");

    // the RaidNode does the raiding inline (instead of submitting to map/reduce)
    if (local) {
      conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    } else {
      conf.set("raid.classname", "org.apache.hadoop.raid.DistRaidNode");
    }
    // use local block fixer
    conf.set("raid.blockfix.classname",
             "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");

    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    // set the purge monitor sleep time to 1 min.
    conf.setLong(PurgeMonitor.PURGE_MONITOR_SLEEP_TIME_KEY, 60000L);
    
    // create a dfs and map-reduce cluster
    final int taskTrackers = 4;

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    mr = new MiniMRCluster(taskTrackers, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);

    Utils.loadTestCodecs(conf);
  }
    
  /**
   * create raid.xml file for RaidNode
   */
  private void mySetup(long targetReplication,
                long metaReplication, long stripeLength) throws Exception {
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<srcPath prefix=\"/user/test/raidtest\"/> " +
                        "<codecId>xor</codecId> " +
                        "<property> " +
                          "<name>targetReplication</name> " +
                          "<value>" + targetReplication + "</value> " +
                          "<description>after RAIDing, decrease the replication factor of a file to this value." +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>metaReplication</name> " +
                          "<value>" + metaReplication + "</value> " +
                          "<description> replication factor of parity file" +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>stripeLength</name> " +
                          "<value>" + stripeLength + "</value> " +
                          "<description> the max number of blocks in a file to RAID together " +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>time_before_har</name> " +
                          "<value>0</value> " +
                          "<description> amount of time waited before har'ing parity files" +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>modTimePeriod</name> " +
                          "<value>2000</value> " + 
                          "<description> time (milliseconds) after a file is modified to make it " +
                                         "a candidate for RAIDing " +
                          "</description> " + 
                        "</property> " +
                     "</policy>" +
                 "</configuration>";
    fileWriter.write(str);
    fileWriter.close();
  }

  /**
   * stop clusters created earlier
   */
  private void stopClusters() throws Exception {
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }

  /**
   * Test that parity files that do not have an associated master file
   * get deleted.
   */
  public void testRaidHar() throws Exception {
    LOG.info("Test testRaidHar  started.");

    long blockSizes    []  = {1024L};
    long stripeLengths []  = {5};
    long targetReplication = 1;
    long metaReplication   = 1;
    int  numBlock          = 9;
    int  iter = 0;

    createClusters(true);
    try {
      for (long blockSize : blockSizes) {
        for (long stripeLength : stripeLengths) {
           doTestHar(iter, targetReplication, metaReplication,
                       stripeLength, blockSize, numBlock);
           iter++;
        }
      }
    } finally {
      stopClusters();
    }
    LOG.info("Test testRaidHar completed.");
  }

  /**
   * Create parity file, delete original file and then validate that
   * parity file is automatically deleted.
   */
  private void doTestHar(int iter, long targetReplication,
                          long metaReplication, long stripeLength,
                          long blockSize, int numBlock) throws Exception {
    LOG.info("doTestHar started---------------------------:" +  " iter " + iter +
             " blockSize=" + blockSize + " stripeLength=" + stripeLength);
    mySetup(targetReplication, metaReplication, stripeLength);
    Path dir = new Path("/user/test/raidtest/subdir/");
    Path file1 = new Path(dir + "/file" + iter);
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/raid/user/test/raidtest/subdir");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      TestRaidNode.createOldFile(fileSys, file1, 1, numBlock, blockSize);
      LOG.info("doTestHar created test files for iteration " + iter);

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      localConf.setInt(RaidNode.RAID_PARITY_HAR_THRESHOLD_DAYS_KEY, 0);
      cnode = RaidNode.createRaidNode(null, localConf);
      FileStatus[] listPaths = null;

      // wait till file is raided
      int count2 = 0;
      Path partFile = null;
      FileStatus partStat = null;
      while (true) {
        try {
          listPaths = fileSys.listStatus(destPath);
          int count = 0;
          Path harPath = null;
          if (listPaths != null) {
            for (FileStatus s : listPaths) {
              LOG.info("doTestHar found path " + s.getPath());
              if (s.getPath().toString().endsWith(".har")) {
                harPath = s.getPath();
                count++;
              }
            }
          }
          if (count == 1  && listPaths.length == 1) {
            partFile = new Path(harPath, "part-0");
            partStat = fileSys.getFileStatus(partFile);
            assertEquals(partStat.getReplication(), targetReplication);
            assertEquals(blockSize, partStat.getBlockSize());
            break;
          }
        } catch (FileNotFoundException e) {
          //ignore
        }
        LOG.info("doTestHar waiting for files to be raided and parity files to be har'ed and deleted. Found " +
                 (listPaths == null ? "none" : listPaths.length));
        Thread.sleep(1000);                  // keep waiting

      }
      
      // delete the source file to test the failure of the reconstruction of
      // the hared parity file.
      fileSys.delete(file1);
      DistributedFileSystem distFS = (DistributedFileSystem)fileSys;
      LocatedBlock block = distFS.getClient().getLocatedBlocks(
          partFile.toUri().getPath(), 0, partStat.getLen()).
          getLocatedBlocks().get(0);
      corruptBlock(block.getBlock(), dfs);
      distFS.getClient().namenode.reportBadBlocks(new LocatedBlock[]{block});
      BlockReconstructor.CorruptBlockReconstructor fixer = 
          new BlockReconstructor.CorruptBlockReconstructor(localConf);
      try {
        fixer.reconstructFile(partFile, null);
        fail("Expected the failure of the block fixer");
      } catch (IOException ex) {
        LOG.warn("Expected failure: " + ex.getMessage(), ex);
      }

      fileSys.delete(dir, true);
      // wait till raid file is deleted
      int count = 1;
      while (count > 0) {
        count = 0;
        try {
          listPaths = fileSys.listStatus(destPath);
          if (listPaths != null) {
            for (FileStatus s : listPaths) {
              LOG.info("doTestHar found path " + s.getPath());
              if (s.getPath().toString().endsWith(".har")) {
                count++;
              }
            }
          }
        } catch (FileNotFoundException e) { } //ignoring
        LOG.info("doTestHar waiting for har file to be deleted. Found " + 
                (listPaths == null ? "none" : listPaths.length) + " files");
        Thread.sleep(1000);
      }
      
    } catch (Exception e) {
      LOG.info("doTestHar Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestHar delete file " + file1);
      fileSys.delete(file1, true);
    }
    LOG.info("doTestHar completed:" + " blockSize=" + blockSize +
             " stripeLength=" + stripeLength);
  }
  
  static void corruptBlock(Block block, MiniDFSCluster dfs) throws IOException {
    boolean corrupted = false;
    for (int i = 0; i < NUM_DATANODES; i++) {
      corrupted |= TestDatanodeBlockScanner.corruptReplica(block, i, dfs);
    }
    assertTrue("could not corrupt block", corrupted);
  }
}
