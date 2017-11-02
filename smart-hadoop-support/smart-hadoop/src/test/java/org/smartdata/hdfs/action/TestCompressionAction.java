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
package org.smartdata.hdfs.action;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.action.MockActionStatusReporter;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.MiniClusterHarness;
import org.smartdata.hdfs.SmartDecompressorStream;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestCompressionAction extends MiniClusterHarness {
  public static final int DEFAULT_BLOCK_SIZE = 1024 * 64;

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  @Override
  @Before
  public void init() throws Exception {
    SmartConf conf = new SmartConf();
    initConf(conf);
    cluster = createCluster(conf);
    // Add namenode URL to smartContext
    conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
      "hdfs://" + cluster.getNameNode().getNameNodeAddressHostPortString());
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = dfs.getClient();
    smartContext = new SmartContext(conf);
  }

  protected void compressoin(String filePath, long bufferSize) throws IOException {
    CompressionAction compressionAction = new CompressionAction();
    compressionAction.setDfsClient(dfsClient);
    compressionAction.setContext(smartContext);
    compressionAction.setStatusReporter(new MockActionStatusReporter());
    Map<String, String> args = new HashMap<>();
    args.put(compressionAction.FILE_PATH, filePath);
    args.put(compressionAction.BUF_SIZE, "" + bufferSize);
    compressionAction.init(args);
    compressionAction.run();
  }

  @Test
  public void testInit() throws IOException {
    Map<String, String> args = new HashMap<>();
    args.put(CompressionAction.FILE_PATH, "/Test");
    args.put(CompressionAction.BUF_SIZE, "1024");
    CompressionAction compressionAction = new CompressionAction();
    compressionAction.init(args);
    compressionAction.setStatusReporter(new MockActionStatusReporter());
  }

  @Test
  public void testExecute() throws Exception {

    String filePath = "/testCompressFile/fadsfa/213";
    int bufferSize = 1024*128;
    byte[] bytes = TestCompressionAction.BytesGenerator.get(bufferSize);

    // Create HDFS file
    OutputStream outputStream = dfsClient.create(filePath, true);
    outputStream.write(bytes);
    outputStream.close();

    // Generate compressed file
    compressoin(filePath, bufferSize);
    HdfsFileStatus fileStatus = dfs.getClient().getFileInfo(filePath);

    // Read compressed file
    byte[] input = new byte[bufferSize];
    DFSInputStream compressedInputStream = dfsClient.open(filePath);
    SmartDecompressorStream uncompressedStream = new SmartDecompressorStream(
      compressedInputStream, new SnappyDecompressor(bufferSize),
      bufferSize);
    int offset = 0;
    while (true) {
      int len = uncompressedStream.read(input, offset , bufferSize - offset);
      if (len <= 0) {
        break;
      }
      offset += len;
    }
    Assert.assertArrayEquals(
      "original array not equals compress/decompressed array", input,bytes
    );
  }

  static final class BytesGenerator {
    private static final byte[] CACHE = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4,
      0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF };
    private static final Random rnd = new Random(12345l);
    private BytesGenerator() {
    }

    public static byte[] get(int size) {
      byte[] array = (byte[]) Array.newInstance(byte.class, size);
      for (int i = 0; i < size; i++)
        array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
      return array;
    }
  }
}
