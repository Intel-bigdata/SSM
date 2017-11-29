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
package org.smartdata.hdfs;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.model.SmartFileCompressionInfo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

/**
 * Test for SmartCompressorStream and SmartDecompressorStream.
 */
public class TestSmartCompressorDecompressorStream extends MiniClusterHarness{
  public static final int DEFAULT_BLOCK_SIZE = 1024*64;
/*
  @Override
  @Before
  public void setup() throws Exception {
    init(DEFAULT_BLOCK_SIZE);
  }

  @Test
  public void testBufferedStream() {
    int BYTE_SIZE = 1024 * 500;
    byte[] bytes1 = BytesGenerator.get(BYTE_SIZE);
    byte[] bytes2 = BytesGenerator.get(BYTE_SIZE);
    byte[] bytes = ArrayUtils.addAll(bytes1, bytes2);
    int bufferSize = 262144;
    int compressionOverhead = (bufferSize / 6) + 32;
    DataOutputStream deflateOut = null;
    DataInputStream inflateIn = null;
    try {
      DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
      SmartFileCompressionInfo compressionInfo = new SmartFileCompressionInfo(
          "", 0, new ArrayList<Long>(), new ArrayList<Long>());
      SmartCompressorStream deflateFilter = new SmartCompressorStream(
          compressedDataBuffer, bufferSize,
          compressionInfo);
      deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));

      deflateOut.write(bytes1, 0, bytes1.length);
      //deflateOut.flush();
      //deflateFilter.finish();
      //deflateOut.write(bytes2, 0, bytes2.length);
      deflateOut.flush();
      deflateFilter.finish();

      DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
      deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
          compressedDataBuffer.getLength());

      CompressionInputStream inflateFilter = new SmartDecompressorStream(
          deCompressedDataBuffer, new SnappyDecompressor(bufferSize),
          bufferSize);

      inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

      byte[] result = new byte[BYTE_SIZE];
      //int skip = inflateIn.skipBytes(compressedPositions.get(0));
      inflateIn.read(result);

      Assert.assertArrayEquals(
          "original array not equals compress/decompressed array", result,
          bytes1);
    } catch (IOException e) {
      fail("testSnappyCompressorDecopressorLogicWithCompressionStreams ex error !!!");
    } finally {
      try {
        if (deflateOut != null)
          deflateOut.close();
        if (inflateIn != null)
          inflateIn.close();
      } catch (Exception e) {
      }
    }
  }

  @Test
  public void testDFSStream() throws IOException {
    int BYTE_SIZE = 1024 * 500;
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);

    // Create HDFS file
    String uncompressedFile = "/file";
    OutputStream outputStream = dfsClient.create(uncompressedFile, true);
    outputStream.write(bytes);
    outputStream.close();

    // Generate compressed file
    int bufferSize = 1024*128;
    String compressedFile = "/file.snappy";
    DFSInputStream inputStream = dfsClient.open(uncompressedFile);
    OutputStream compressedOutputStream = dfsClient.create(compressedFile, true);
    SmartFileCompressionInfo compressionInfo = new SmartFileCompressionInfo(
        "", 0, new ArrayList<Long>(), new ArrayList<Long>());
    SmartCompressorStream smartCompressorStream = new SmartCompressorStream(
        compressedOutputStream, bufferSize,
        compressionInfo);
    byte[] input = new byte[BYTE_SIZE];
    while (true) {
      int len = inputStream.read(input, 0, BYTE_SIZE);
      if (len == -1) {
        break;
      }
      smartCompressorStream.write(input, 0, len);
    }
    smartCompressorStream.finish();
    compressedOutputStream.close();

    // Read compressed file
    DFSInputStream compressedInputStream = dfsClient.open(compressedFile);
    SmartDecompressorStream uncompressedStream = new SmartDecompressorStream(
        compressedInputStream, new SnappyDecompressor(bufferSize),
        bufferSize);
    int offset = 0;
    while (true) {
      int len = uncompressedStream.read(input, offset , BYTE_SIZE - offset);
      if (len <= 0) {
        break;
      }
      offset += len;
    }

    Assert.assertArrayEquals(
        "original array not equals compress/decompressed array", input,
        bytes);
  }

  static final class BytesGenerator {
    private BytesGenerator() {
    }

    private static final byte[] CACHE = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4,
        0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF };
    private static final Random rnd = new Random(12345l);

    public static byte[] get(int size) {
      byte[] array = (byte[]) Array.newInstance(byte.class, size);
      for (int i = 0; i < size; i++)
        array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
      return array;
    }
  }*/
}
