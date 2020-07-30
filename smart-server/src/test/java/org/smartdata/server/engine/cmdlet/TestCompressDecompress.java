/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.server.engine.cmdlet;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.CompressionCodec;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hadoop.filesystem.SmartFileSystem;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.hdfs.scheduler.CompressionScheduler;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.FileState;
import org.smartdata.model.action.ActionScheduler;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.CmdletManager;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TestCompressDecompress extends MiniSmartClusterHarness {
  private DFSClient smartDFSClient;
  private String codec;

  @Override
  @Before
  public void init() throws Exception {
    DEFAULT_BLOCK_SIZE = 1024 * 1024;
    super.init();
//    this.compressionImpl = "snappy";
//    this.compressionImpl = "Lz4";
//    this.compressionImpl = "Bzip2";
    this.codec = CompressionCodec.ZLIB;
    smartDFSClient = new SmartDFSClient(ssm.getContext().getConf());
  }

  @Test
  public void testSubmitCompressionAction() throws Exception {
    // if (!loadedNative()) {
    //   return;
    // }
    waitTillSSMExitSafeMode();

    // initDB();
    int arraySize = 1024 * 1024 * 80;
    String fileName = "/ssm/compression/file1";
    byte[] bytes = prepareFile(fileName, arraySize);
    MetaStore metaStore = ssm.getMetaStore();

    int bufSize = 1024 * 1024 * 10;
    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
        + " -bufSize " + bufSize + " -codec " + codec);

    waitTillActionDone(cmdId);
    FileState fileState = null;
    // metastore  test
    int n = 0;
    while (true) {
      fileState = metaStore.getFileState(fileName);
      if (FileState.FileType.COMPRESSION.equals(fileState.getFileType())) {
        break;
      }
      Thread.sleep(1000);
      if (n++ >= 20) {
        throw new Exception("Time out in waiting for getting expect file state.");
      }
    }

    Assert.assertEquals(FileState.FileStage.DONE, fileState.getFileStage());
    Assert.assertTrue(fileState instanceof CompressionFileState);
    CompressionFileState compressionFileState = (CompressionFileState) fileState;
    Assert.assertEquals(fileName, compressionFileState.getPath());
    Assert.assertEquals(bufSize, compressionFileState.getBufferSize());
    Assert.assertEquals(codec, compressionFileState.getCompressionImpl());
    Assert.assertEquals(arraySize, compressionFileState.getOriginalLength());
    Assert.assertTrue(compressionFileState.getCompressedLength() > 0);
    Assert.assertTrue(compressionFileState.getCompressedLength()
        < compressionFileState.getOriginalLength());

    // data accuracy test
    byte[] input = new byte[arraySize];
    DFSInputStream dfsInputStream = smartDFSClient.open(fileName);
    int offset = 0;
    while (true) {
      int len = dfsInputStream.read(input, offset, arraySize - offset);
      if (len <= 0) {
        break;
      }
      offset += len;
    }
    Assert.assertArrayEquals(
        "original array not equals compress/decompressed array", input, bytes);
  }

//  @Test(timeout = 90000)
//  public void testCompressEmptyFile() throws Exception {
//    waitTillSSMExitSafeMode();
//
//    // initDB();
//    String fileName = "/ssm/compression/file2";
//    prepareFile(fileName, 0);
//    MetaStore metaStore = ssm.getMetaStore();
//
//    int bufSize = 1024 * 1024;
//    CmdletManager cmdletManager = ssm.getCmdletManager();
//    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
//        + " -bufSize " + bufSize + " -compressImpl " + compressionImpl);
//
//    waitTillActionDone(cmdId);
//    FileState fileState = metaStore.getFileState(fileName);
//    while (!fileState.getFileType().equals(FileState.FileType.COMPRESSION)) {
//      Thread.sleep(200);
//      fileState = metaStore.getFileState(fileName);
//    }
//
//    // metastore  test
////    Assert.assertEquals(FileState.FileType.COMPRESSION, fileState.getFileType());
//    Assert.assertEquals(FileState.FileStage.DONE, fileState.getFileStage());
//    Assert.assertTrue(fileState instanceof CompressionFileState);
//    CompressionFileState compressionFileState = (CompressionFileState) fileState;
//    Assert.assertEquals(fileName, compressionFileState.getPath());
//    Assert.assertEquals(bufSize, compressionFileState.getBufferSize());
//    Assert.assertEquals(compressionImpl, compressionFileState.getCompressionImpl());
//    Assert.assertEquals(0, compressionFileState.getOriginalLength());
//    Assert.assertEquals(0, compressionFileState.getCompressedLength());
//
//    // File length test
//    Assert.assertEquals(0, dfsClient.getFileInfo(fileName).getLen());
//  }

  @Test
  public void testCompressedFileRandomRead() throws Exception {
    // if (!loadedNative()) {
    //   return;
    // }
    waitTillSSMExitSafeMode();

    // initDB();
    int arraySize = 1024 * 1024 * 8;
    String fileName = "/ssm/compression/file3";
    byte[] bytes = prepareFile(fileName, arraySize);

    int bufSize = 1024 * 1024;
    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
      + " -bufSize " + bufSize + " -codec " + codec);

    waitTillActionDone(cmdId);

    // Test random read
    Random rnd = new Random(System.currentTimeMillis());
    DFSInputStream dfsInputStream = smartDFSClient.open(fileName);
    int randomReadSize = 500;
    byte[] randomReadBuffer = new byte[randomReadSize];
    for (int i = 0; i < 5; i++) {
      int pos = rnd.nextInt(arraySize - 500);
      byte[] subBytes = Arrays.copyOfRange(bytes, pos, pos + 500);
      dfsInputStream.seek(pos);
      Assert.assertEquals(pos, dfsInputStream.getPos());
      int off = 0;
      while (off < randomReadSize) {
        int len = dfsInputStream.read(randomReadBuffer, off, randomReadSize - off);
        off += len;
      }
      Assert.assertArrayEquals(subBytes, randomReadBuffer);
      Assert.assertEquals(pos + 500, dfsInputStream.getPos());
    }
  }

  @Test
  public void testDecompress() throws Exception {
    int arraySize = 1024 * 1024 * 8;
    String filePath = "/ssm/compression/file4";
    prepareFile(filePath, arraySize);
    dfsClient.setStoragePolicy(filePath, "COLD");
    HdfsFileStatus fileStatusBefore = dfsClient.getFileInfo(filePath);

    CmdletManager cmdletManager = ssm.getCmdletManager();
    // Expect that a common file cannot be decompressed.
    List<ActionScheduler> schedulers = cmdletManager.getSchedulers("decompress");
    Assert.assertTrue(schedulers.size() == 1);
    ActionScheduler scheduler = schedulers.get(0);
    Assert.assertTrue(scheduler instanceof CompressionScheduler);
    Assert.assertFalse(((CompressionScheduler) scheduler).supportDecompression(filePath));

    // Compress the given file
    long cmdId = cmdletManager.submitCmdlet(
        "compress -file " + filePath + " -codec " + codec);
    waitTillActionDone(cmdId);
    FileState fileState = HadoopUtil.getFileState(dfsClient, filePath);
    Assert.assertTrue(fileState instanceof CompressionFileState);

    // The storage policy should not be changed
    HdfsFileStatus fileStatusAfterCompress = dfsClient.getFileInfo(filePath);
    if (fileStatusBefore.getStoragePolicy() != 0) {
      // To make sure the consistency of storage policy
      Assert.assertEquals(fileStatusBefore.getStoragePolicy(),
          fileStatusAfterCompress.getStoragePolicy());
    }

    // Try to decompress a compressed file
    cmdId = cmdletManager.submitCmdlet("decompress -file " + filePath);
    waitTillActionDone(cmdId);
    fileState = HadoopUtil.getFileState(dfsClient, filePath);
    Assert.assertFalse(fileState instanceof CompressionFileState);

    // The storage policy should not be changed.
    HdfsFileStatus fileStatusAfterDeCompress = dfsClient.getFileInfo(filePath);
    if (fileStatusBefore.getStoragePolicy() != 0) {
      // To make sure the consistency of storage policy
      Assert.assertEquals(fileStatusBefore.getStoragePolicy(),
          fileStatusAfterDeCompress.getStoragePolicy());
    }
  }

  @Test
  public void testCompressDecompressDir() throws Exception {
    String dir = "/ssm/compression";
    dfsClient.mkdirs(dir, null, true);
    CmdletManager cmdletManager = ssm.getCmdletManager();

    List<ActionScheduler> schedulers = cmdletManager.getSchedulers(
        "decompress");
    Assert.assertTrue(schedulers.size() == 1);
    ActionScheduler scheduler = schedulers.get(0);
    Assert.assertTrue(scheduler instanceof CompressionScheduler);
    // Expect that a dir cannot be compressed.
    Assert.assertFalse(((
        CompressionScheduler) scheduler).supportCompression(dir));
    // Expect that a dir cannot be decompressed.
    Assert.assertFalse(((
        CompressionScheduler) scheduler).supportDecompression(dir));
  }

  @Test
  public void testCheckCompressAction() throws Exception {
    int arraySize = 1024 * 1024 * 8;
    String fileDir = "/ssm/compression/";
    String fileName = "file5";
    String filePath = fileDir + fileName;
    prepareFile(filePath, arraySize);
    CmdletManager cmdletManager = ssm.getCmdletManager();

    long cmdId = cmdletManager.submitCmdlet(
        "checkcompress -file " + filePath);
    waitTillActionDone(cmdId);

    // Test directory case.
    cmdId = cmdletManager.submitCmdlet("checkcompress -file " + fileDir);
    waitTillActionDone(cmdId);
  }

  @Test
  public void testListLocatedStatus() throws Exception {
    // if (!loadedNative()) {
    //   return;
    // }
    waitTillSSMExitSafeMode();

    // initDB();
    SmartFileSystem smartDfs = new SmartFileSystem();
    smartDfs.initialize(dfs.getUri(), ssm.getContext().getConf());

    int arraySize = 1024 * 1024 * 8;
    String fileName = "/ssm/compression/file4";
    byte[] bytes = prepareFile(fileName, arraySize);

    // For uncompressed file, SmartFileSystem and DistributedFileSystem behave exactly the same
    RemoteIterator<LocatedFileStatus> iter1 = dfs.listLocatedStatus(new Path(fileName));
    LocatedFileStatus stat1 = iter1.next();
    RemoteIterator<LocatedFileStatus> iter2 = smartDfs.listLocatedStatus(new Path(fileName));
    LocatedFileStatus stat2 = iter2.next();
    Assert.assertEquals(stat1.getPath(), stat2.getPath());
    Assert.assertEquals(stat1.getBlockSize(), stat2.getBlockSize());
    Assert.assertEquals(stat1.getLen(), stat2.getLen());
    BlockLocation[] blockLocations1 = stat1.getBlockLocations();
    BlockLocation[] blockLocations2 = stat2.getBlockLocations();
    Assert.assertEquals(blockLocations1.length, blockLocations2.length);
    for (int i = 0; i < blockLocations1.length; i++) {
      Assert.assertEquals(blockLocations1[i].getLength(), blockLocations2[i].getLength());
      Assert.assertEquals(blockLocations1[i].getOffset(), blockLocations2[i].getOffset());
    }

    // Test compressed file
    int bufSize = 1024 * 1024;
    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
      + " -bufSize " + bufSize + " -codec " + codec);
    waitTillActionDone(cmdId);
    RemoteIterator<LocatedFileStatus> iter3 = dfs.listLocatedStatus(new Path(fileName));
    LocatedFileStatus stat3 = iter3.next();
    BlockLocation[] blockLocations3 = stat3.getBlockLocations();
    RemoteIterator<LocatedFileStatus> iter4 = smartDfs.listLocatedStatus(new Path(fileName));
    LocatedFileStatus stat4 = iter4.next();
    BlockLocation[] blockLocations4 = stat4.getBlockLocations();
    Assert.assertEquals(stat1.getPath(), stat4.getPath());
    Assert.assertEquals(stat1.getBlockSize(), stat4.getBlockSize());
    Assert.assertEquals(stat1.getLen(), stat4.getLen());
  }

  @Test
  public void testRename() throws Exception {
    // Create raw file
    Path path = new Path("/test/compress_files/");
    dfs.mkdirs(path);
    int rawLength = 1024 * 1024 * 8;
    String fileName = "/test/compress_files/file_0";
    DFSTestUtil.createFile(dfs, new Path(fileName),
        rawLength, (short) 1, 1);
    int bufSize = 1024 * 1024;
    waitTillSSMExitSafeMode();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    // Compress files
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
        + " -bufSize " + bufSize + " -codec " + codec);
    waitTillActionDone(cmdId);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    smartDFSClient.rename("/test/compress_files/file_0",
        "/test/compress_files/file_4");
    Assert.assertTrue(smartDFSClient.exists("/test/compress_files/file_4"));
    HdfsFileStatus fileStatus =
        smartDFSClient.getFileInfo("/test/compress_files/file_4");
    Assert.assertEquals(rawLength, fileStatus.getLen());
  }

  @Test
  public void testUnsupportedMethod() throws Exception {
    // Concat, truncate and append are not supported
    // Create raw file
    Path path = new Path("/test/compress_files/");
    dfs.mkdirs(path);
    int rawLength = 1024 * 1024 * 8;
    String fileName = "/test/compress_files/file_0";
    DFSTestUtil.createFile(dfs, new Path(fileName),
        rawLength, (short) 1, 1);
    int bufSize = 1024 * 1024;
    waitTillSSMExitSafeMode();
    CmdletManager cmdletManager = ssm.getCmdletManager();
    // Compress files
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
        + " -bufSize " + bufSize + " -codec " + codec);
    waitTillActionDone(cmdId);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    // Test unsupported methods on compressed file
    try {
      smartDFSClient.concat(fileName + "target", new String[]{fileName});
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Compressed"));
    }
    /*try {
      smartDFSClient.truncate(fileName, 100L);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Compressed"));
    }*/
  }

  private void waitTillActionDone(long cmdId) throws Exception {
    int n = 0;
    while (true) {
      Thread.sleep(1000);
      CmdletManager cmdletManager = ssm.getCmdletManager();
      CmdletInfo info = cmdletManager.getCmdletInfo(cmdId);
      if (info == null) {
        continue;
      }
      CmdletState state = info.getState();
      if (state == CmdletState.DONE) {
        return;
      } else if (state == CmdletState.FAILED) {
        // Reasonably assume that there is only one action wrapped by a given cmdlet.
        long aid = cmdletManager.getCmdletInfo(cmdId).getAids().get(0);
        Assert.fail(
            "Action failed. " + cmdletManager.getActionInfo(aid).getLog());
      } else {
        System.out.println(state);
      }
      // Wait for 20s.
      if (++n == 20) {
        throw new Exception("Time out in waiting for cmdlet: " + cmdletManager.
            getCmdletInfo(cmdId).toString());
      }
    }
  }

  private byte[] prepareFile(String fileName, int fileSize) throws IOException {
    byte[] bytes = TestCompressDecompress.BytesGenerator.get(fileSize);

    // Create HDFS file
    OutputStream outputStream = dfsClient.create(fileName, true);
    outputStream.write(bytes);
    outputStream.close();

    return bytes;
  }

  static final class BytesGenerator {
    private static final byte[] CACHE = new byte[]{0x0, 0x1, 0x2, 0x3, 0x4,
      0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF};
    private static final Random rnd = new Random(12345L);

    private BytesGenerator() {
    }

    public static byte[] get(int size) {
      byte[] array = (byte[]) Array.newInstance(byte.class, size);
      for (int i = 0; i < size; i++) {
        array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
      }
      return array;
    }
  }

  private boolean loadedNative() {
    return CompressionCodec.getNativeCodeLoaded();
  }
}
