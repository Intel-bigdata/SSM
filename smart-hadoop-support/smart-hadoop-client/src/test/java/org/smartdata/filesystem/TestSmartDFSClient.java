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

package org.smartdata.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.MiniClusterHarness;
import org.smartdata.hdfs.client.SmartDFSClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TestSmartDFSClient extends MiniClusterHarness {
  Map<String, List<Long>> metaData = new HashMap<>();

  @Before
  @Override
  public void init() throws Exception {
    SmartConf conf = new SmartConf();
    initConf(conf);
    cluster = createCluster(conf);
    conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
            "hdfs://" + cluster.getNameNode().getNameNodeAddressHostPortString());
    conf.set(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY,
            "hdfs://" + cluster.getNameNode().getNameNodeAddressHostPortString());
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = dfs.getClient();
    smartContext = new SmartContext(conf);
    createTestFiles();
  }

  private void initConf(Configuration conf) {
    conf.setStrings(SmartConfKeys.SMART_SMALL_FILE_DIRS_KEY, "/test/smallfile");
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  public void createTestFiles() throws Exception {
    Path path = new Path("/test/smallfile/testFile");
    long pos = 0L;
    if (!dfs.mkdirs(path.getParent())) {
      throw new IOException("Mkdirs failed to create " +
              path.getParent().toString());
    }
    FSDataOutputStream out = null;
    try {
      out = dfs.create(path, (short) 1);
      for (int i = 0; i < 100000; i++) {
        String fileName = "/test/smallfile/file" + i;
        long fileLen = (10 + (int) (Math.random() * 11)) * 1024;
        byte[] toWrite = new byte[1024];
        Random rb = new Random(2017);
        long bytesToWrite = fileLen;
        while (bytesToWrite>0) {
          rb.nextBytes(toWrite);
          int bytesToWriteNext = (1024<bytesToWrite)?1024:(int)bytesToWrite;

          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
        ArrayList<Long> fileInfo = new ArrayList<>();
        fileInfo.add(pos);
        fileInfo.add(fileLen);
        metaData.put(fileName, fileInfo);
        pos += fileLen;
      }
      out.close();
      out = null;
    } finally {
      IOUtils.closeStream(out);
    }
  }


  @Test
  public void testGetAllBlocks() throws Exception {
    String path = "/test/smallfile/fileTest";
    Path filePath = new Path(path);
    DFSTestUtil.createFile(dfs, filePath, 100L, (byte) 1, 2017);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    SmartDFSInputStream is = smartDFSClient.open(path);
    Assert.assertEquals(1, is.getAllBlocks().size());
    is.close();
  }

  @Test
  public void testGetFileLen() throws Exception {
    String path = "/test/smallfile/file96";
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf(), metaData);
    SmartDFSInputStream is = smartDFSClient.open(path);
    Assert.assertEquals((long) metaData.get(path).get(1), is.getFileLength());
    is.close();
  }

  @Test
  public void testGetPos() throws Exception {
    String path = "/test/smallfile/fileReadByteArray";
    Path filePath = new Path(path);
    DFSTestUtil.createFile(dfs, filePath, 100L, (byte) 1, 2017);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    SmartDFSInputStream is = smartDFSClient.open(path);
    byte[] bytes = new byte[100];
    is.read(bytes);
    Assert.assertEquals(6, is.getPos());
    is.close();
  }

  @Test
  public void testByteRead() throws Exception {
    String path = "/test/smallfile/fileReadOneByte";
    Path filePath = new Path(path);
    DFSTestUtil.createFile(dfs, filePath, 100L, (byte) 1, 2017);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    SmartDFSInputStream is = smartDFSClient.open(path);
    Assert.assertEquals(158, is.read());
    is.close();
  }

  @Test
  public void testByteArrayRead() throws Exception {
    String path = "/test/smallfile/fileReadByteArray";
    Path filePath = new Path(path);
    DFSTestUtil.createFile(dfs, filePath, 100L, (byte) 1, 2017);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    SmartDFSInputStream is = smartDFSClient.open(path);
    byte[] bytes = new byte[100];
    Assert.assertEquals(6, is.read(bytes));
    is.close();
  }

  @Test
  public void testByteBuffer() throws Exception {
    String path = "/test/smallfile/fileReadByteArray";
    Path filePath = new Path(path);
    DFSTestUtil.createFile(dfs, filePath, 100L, (byte) 1, 2017);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    SmartDFSInputStream is = smartDFSClient.open(path);
    byte[] bytes = new byte[100];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    Assert.assertEquals(6, is.read(bytes));
    is.close();
  }

  @Test
  public void testBytesRead() throws Exception {
    String path = "/test/smallfile/fileReadByteArray";
    Path filePath = new Path(path);
    DFSTestUtil.createFile(dfs, filePath, 100L, (byte) 1, 2017);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    SmartDFSInputStream is = smartDFSClient.open(path);
    byte[] bytes = new byte[100];
    Assert.assertEquals(5, is.read(bytes, 1, 5));
    is.close();
  }

  @Test
  public void testPositionRead() throws Exception {
    String path = "/test/smallfile/fileReadByteArray";
    Path filePath = new Path(path);
    DFSTestUtil.createFile(dfs, filePath, 100L, (byte) 1, 2017);
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    SmartDFSInputStream is = smartDFSClient.open(path);
    byte[] bytes = new byte[100];
    Assert.assertEquals(4, is.read(2, bytes, 1, 5));
    is.close();
  }

  //@Test
  public void testReadSmallFile() throws Exception {
    System.out.println("11111111111111");
    String[] files = new String[] {
        "/test/smallfile/fileA", "/test/smallfile/fileB", "/test/smallfile/fileC", "/test/smallfile/fileD"
    };
    long[] lengths = new long[] {100, 20, 100, 200};
    int[] readCounts = new int[] {1, 10, 1, 10};
    for (int i = 0; i < files.length; i++) {
      DFSTestUtil.createFile(dfs, new Path(files[i]), lengths[i], (byte) 1, 2017);
    }
    SmartDFSClient smartDFSClient = new SmartDFSClient(smartContext.getConf());
    for (int i = 0; i < files.length; i++) {
      System.out.println("222222222222222222");
      for (int j = 0; j < readCounts[j]; j++) {
        SmartDFSInputStream is = smartDFSClient.open(files[i]);
        is.getAllBlocks();
        is.getFileLength();
        for (int k = 0; k < is.getFileLength(); k++) {
          int a = is.read();
          System.out.println("a:" + a);
          System.out.println("pos: " + is.getPos());
        }
        /*byte[] bytes = new byte[1000];
        int b = is.read(bytes);
        System.out.println(is.getPos());
        System.out.println("b:" + b);
        System.out.println("position: " + is.getPos());
        *//*int c = is.read(1, bytes, 2, 10);
        System.out.println("c: " + c);*//*
        System.out.println(is.getReadStatistics().getTotalBytesRead());
        System.out.println(is.getReadStatistics().getRemoteBytesRead());
        System.out.println(is.getReadStatistics().getTotalLocalBytesRead());
        System.out.println(is.getReadStatistics().getTotalShortCircuitBytesRead());
        System.out.println(is.getReadStatistics().getTotalZeroCopyBytesRead());*/
        is.close();
      }
    }
  }
}
