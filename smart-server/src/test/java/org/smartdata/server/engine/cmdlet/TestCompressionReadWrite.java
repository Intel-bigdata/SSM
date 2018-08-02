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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hadoop.filesystem.SmartFileSystem;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.CmdletState;
import org.smartdata.model.CompressionFileState;
import org.smartdata.model.FileState;
import org.smartdata.server.MiniSmartClusterHarness;
import org.smartdata.server.engine.CmdletManager;

import java.io.File;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;

public class TestCompressionReadWrite extends MiniSmartClusterHarness {
  private DFSClient smartDFSClient;
  private String compressionImpl;

  @Override
  @Before
  public void init() throws Exception {
    DEFAULT_BLOCK_SIZE = 1024 * 1024;
    super.init();
//    this.compressionImpl = "snappy";
//    this.compressionImpl = "Lz4";
//    this.compressionImpl = "Bzip2";
    this.compressionImpl = "Zlib";
    smartDFSClient = new SmartDFSClient(ssm.getContext().getConf());
  }

  @Test
  public void testSubmitCompressionAction() throws Exception {
    // if (!loadedNative()) {
    //   return;
    // }
    waitTillSSMExitSafeMode();

    // initDB();
    int arraySize = 1024 * 1024 * 8;
    String fileName = "/ssm/compression/file1";
    byte[] bytes = prepareFile(fileName, arraySize);
    MetaStore metaStore = ssm.getMetaStore();

    int bufSize = 1024 * 1024;
    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
        + " -bufSize " + bufSize + " -compressImpl " + compressionImpl);

    waitTillActionDone(cmdId);
    Thread.sleep(1000);
    // metastore  test
    FileState fileState = metaStore.getFileState(fileName);
    Assert.assertEquals(FileState.FileType.COMPRESSION, fileState.getFileType());
    Assert.assertEquals(FileState.FileStage.DONE, fileState.getFileStage());
    Assert.assertTrue(fileState instanceof CompressionFileState);
    CompressionFileState compressionFileState = (CompressionFileState) fileState;
    Assert.assertEquals(fileName, compressionFileState.getPath());
    Assert.assertEquals(bufSize, compressionFileState.getBufferSize());
    Assert.assertEquals(compressionImpl, compressionFileState.getCompressionImpl());
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
    Assert.assertArrayEquals("original array not equals compress/decompressed array", input, bytes);
  }

  @Test
  public void testCompressEmptyFile() throws Exception {
    waitTillSSMExitSafeMode();

    // initDB();
    String fileName = "/ssm/compression/file1";
    prepareFile(fileName, 0);
    MetaStore metaStore = ssm.getMetaStore();

    int bufSize = 1024 * 1024;
    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
        + " -bufSize " + bufSize + " -compressImpl " + compressionImpl);

    waitTillActionDone(cmdId);
    Thread.sleep(500);
    // metastore  test
    FileState fileState = metaStore.getFileState(fileName);
    Assert.assertEquals(FileState.FileType.COMPRESSION, fileState.getFileType());
    Assert.assertEquals(FileState.FileStage.DONE, fileState.getFileStage());
    Assert.assertTrue(fileState instanceof CompressionFileState);
    CompressionFileState compressionFileState = (CompressionFileState) fileState;
    Assert.assertEquals(fileName, compressionFileState.getPath());
    Assert.assertEquals(bufSize, compressionFileState.getBufferSize());
    Assert.assertEquals(compressionImpl, compressionFileState.getCompressionImpl());
    Assert.assertEquals(0, compressionFileState.getOriginalLength());
    Assert.assertEquals(0, compressionFileState.getCompressedLength());

    // File length test
    Assert.assertEquals(0, dfsClient.getFileInfo(fileName).getLen());
  }

  @Test
  public void testCompressedFileRandomRead() throws Exception {
    // if (!loadedNative()) {
    //   return;
    // }
    waitTillSSMExitSafeMode();

    // initDB();
    int arraySize = 1024 * 1024 * 8;
    String fileName = "/ssm/compression/file1";
    byte[] bytes = prepareFile(fileName, arraySize);

    int bufSize = 1024 * 1024;
    CmdletManager cmdletManager = ssm.getCmdletManager();
    long cmdId = cmdletManager.submitCmdlet("compress -file " + fileName
      + " -bufSize " + bufSize + " -compressImpl " + compressionImpl);

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
  public void testListLocatedStatus() throws Exception {
    // if (!loadedNative()) {
    //   return;
    // }
    waitTillSSMExitSafeMode();

    // initDB();
    SmartFileSystem smartDfs = new SmartFileSystem();
    smartDfs.initialize(dfs.getUri(), ssm.getContext().getConf());

    int arraySize = 1024 * 1024 * 8;
    String fileName = "/ssm/compression/file1";
    byte[] bytes = prepareFile(fileName, arraySize);

    // For uncompressed file, SmartFileSystem and DistributedFileSystem behave exactly the same
    RemoteIterator<LocatedFileStatus> iter1 = dfs.listLocatedStatus(new Path("/ssm/compression"));
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
      + " -bufSize " + bufSize + " -compressImpl " + compressionImpl);
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

    return;
  }

  private void waitTillActionDone(long cmdId) throws Exception {
    while (true) {
      Thread.sleep(1000);
      CmdletManager cmdletManager = ssm.getCmdletManager();
      CmdletState state = cmdletManager.getCmdletInfo(cmdId).getState();
      if (state == CmdletState.DONE) {
        return;
      } else if (state == CmdletState.FAILED) {
        Assert.fail("Compression action failed.");
      } else {
        System.out.println(state);
      }
    }
  }

  private byte[] prepareFile(String fileName, int fileSize) throws Exception {
    byte[] bytes = TestCompressionReadWrite.BytesGenerator.get(fileSize);

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
    String hadoopNativePath = "";
    //hadoopnativePath used to suport Bzip2 compresionImpl
    try {
      if (!(System.getenv("HADOOP_HOME") == null)) {
        hadoopNativePath =
            System.getenv("HADOOP_HOME") + "/lib/native/libhadoop.so";
      } else {
        hadoopNativePath =
            System.getenv("HADOOP_COMMON_HOME") + "/lib/native/libhadoop.so";
      }
    } catch (Exception e) {
      return false;
    }
    if (hadoopNativePath.isEmpty() || !new File(hadoopNativePath).isFile()) {
      return false;
    }
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      return false;
    }
    return true;
  }
}
