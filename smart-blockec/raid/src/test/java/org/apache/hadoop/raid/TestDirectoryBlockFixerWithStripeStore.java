package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.mapred.MiniMRCluster;

import junit.framework.TestCase;

public class TestDirectoryBlockFixerWithStripeStore extends TestCase {
  
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestDirectoryBlockFixerWithStripeStore");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CHECKSUM_STORE_DIR = new File(TEST_DIR,
      "ckm_store." + System.currentTimeMillis()).getAbsolutePath();
  final static String STRIPE_STORE_DIR = new File(TEST_DIR,
      "stripe_store." + System.currentTimeMillis()).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  final long blockSize = 8192L;
  final long[] fileSizes =
      new long[]{blockSize + blockSize/2, // block 0, 1
      3*blockSize,                  // block 2, 3
      blockSize + blockSize/2 + 1}; // block 4, 5, 6, 7
  final long[] blockSizes = new long[]{blockSize, 2*blockSize, blockSize/2};
  final Integer[] rsCorruptFileIdx1 = new Integer[]{0, 1, 2, 3, 5, 6, 7};
  final int[] rsNumCorruptBlocksInFiles1 = new int[] {2, 2, 3};
  final Integer[] rsCorruptFileIdx2 = new Integer[]{1, 2, 3, 4, 5, 6};
  final int[] rsNumCorruptBlocksInFiles2 = new int[] {1, 2, 3};
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  String hftp = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  Random rand = new Random();
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
  }
  
  private void mySetup(int stripeLength) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.set("mapred.raid.http.address", "localhost:0");

    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1, 3, "/destraid",
        "/destraidrs", false, true);

    conf.setBoolean("dfs.permissions", false);

    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    hftp = "hftp://localhost.localdomain:" + dfsCluster.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set(RaidNode.RAID_CHECKSUM_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalChecksumStore");
    conf.setBoolean(RaidNode.RAID_CHECKSUM_STORE_REQUIRED_KEY, true);
    conf.set(LocalChecksumStore.LOCAL_CHECK_STORE_DIR_KEY, CHECKSUM_STORE_DIR);
    conf.set(RaidNode.RAID_STRIPE_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalStripeStore");
    conf.set(LocalStripeStore.LOCAL_STRIPE_STORE_DIR_KEY, STRIPE_STORE_DIR);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("RaidTest1", "/user/dhruba/raidtest",
        1, 1);
    cb.addPolicy("RaidTest2", "/user/dhruba/raidtestrs",
        1, 1, "rs");
    cb.persist();
  }
  
  public void testDirBlockFixWithDeletingFiles() throws Exception {
    implDirBlockFixLocal(0);
  }
  
  public void testDirBlockFixWithAddingFiles() throws Exception {
    implDirBlockFixLocal(1);
  }
  
  public void testDirBlockFixWithRenamingFiles() throws Exception {
    implDirBlockFixLocal(2);
  }
  
  private void implDirBlockFixLocal(int operatorId) 
      throws Exception {
    int stripeLength = 3;
    mySetup(stripeLength);
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    
    Path dirPath =new Path("/user/dhruba/raidtestrs");
    Path[] files = TestRaidDfs.createTestFiles(dirPath, fileSizes, 
        blockSizes, crcs, seeds, fileSys, (short)1);
    
    Path destPath = new Path("/destraidrs/user/dhruba");
    Configuration localConf = this.getRaidNodeConfig(conf);
    
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath, destPath);
      cnode.stop(); cnode.join();
      
      DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      // corrupt files
      this.corruptFiles(dirPath, crcs, rsCorruptFileIdx1, dfs, files, 
          rsNumCorruptBlocksInFiles1);
      
      FileStatus dirStat = dfs.getFileStatus(dirPath);
      
      int filesToFix = 3;
      if (operatorId == 0) {
        // delete file
        dfs.delete(files[0], true);
        filesToFix = 2;
        LOG.info("Delete file: " + files[0]);
      } else if (operatorId == 1) {
        // add file
        TestRaidDfs.createTestFile(dfs, new Path(dirPath, "file3"), (short)1, 
                          5, blockSize);
      } else { 
        // rename file
        Path newPath = new Path(files[1].toUri().getPath() + "_rename");
        dfs.rename(files[1], newPath);
        LOG.info("Rename file: " + files[1]);
        files[1] = newPath;
      }
      FileStatus newDirStat = dfs.getFileStatus(dirPath);
      // assert the modification is changed.
      assertNotSame(dirStat.getModificationTime(), 
          newDirStat.getModificationTime());
      
      Codec codec = Codec.getCodec("rs");
      ParityFilePair pfPair = ParityFilePair.getParityFile(codec, newDirStat, 
          localConf);
      
      assertNull(pfPair);
      
      cnode = RaidNode.createRaidNode(null, localConf);
      // check the block fixing.
      long start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < filesToFix &&
          System.currentTimeMillis() - start < 240000) {
        Thread.sleep(1000);
        LOG.info("Test testDirBlockFix waiting for " + filesToFix + 
            " files to be fixed, " +
            "current: " + cnode.blockIntegrityMonitor.getNumFilesFixed());
      }
      
      dfs = TestDirectoryBlockFixer.getDFS(conf, dfs);
      for (int i = 1; i < fileSizes.length; i++) {
        assertTrue("file " + files[i] + " not fixed",
            TestRaidDfs.validateFile(dfs, files[i], fileSizes[i],
              crcs[i]));
      }
      
      // wait for the re-generation of the parity files
      TestRaidDfs.waitForDirRaided(LOG, dfs, dirPath, destPath);
    } catch (Exception e) {
      throw e;
    } finally {
      myTearDown();
    }
  }
  
  public Configuration getRaidNodeConfig(Configuration conf) {
    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    return localConf;
  }
  
  private void corruptFiles(Path dirPath, long[] crcs, 
      Integer[] corruptBlockIdxs, DistributedFileSystem dfs,
      Path[] files, int[] numCorruptBlocksInFiles) throws IOException {
    int totalCorruptFiles = DFSUtil.getCorruptFiles(dfs).length;
    TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirPath,
        crcs, corruptBlockIdxs, fileSys, dfsCluster, false, true);
    
    String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
    for (int i = 0; i < numCorruptBlocksInFiles.length; i++) {
      if (numCorruptBlocksInFiles[i] > 0)
        totalCorruptFiles++;
    }
    assertEquals("files not corrupted", totalCorruptFiles,
        corruptFiles.length);
    for (int i = 0; i< fileSizes.length; i++) {
      assertEquals("wrong number of corrupt blocks for file " + 
          files[i], numCorruptBlocksInFiles[i],
          RaidDFSUtil.corruptBlocksInFile(dfs,
          files[i].toUri().getPath(), 0, fileSizes[i]).size());
    }
  }
  
  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (dfsCluster != null) { dfsCluster.shutdown(); }
  }
}
