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
package org.smartdata.model;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestSmartFileCompressionInfo {
  private SmartFileCompressionInfo compressionInfo;
  private final String fileName = "/testFile";
  private final int bufferSize = 10;
  private final long originalLength = 86;
  private final long compressedLength = 34;
  private final Long[] originalPos = {0L, 10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L};
  private final Long[] compressedPos = {0L, 4L, 10L, 13L, 19L, 22L, 25L, 28L, 32L};


  @Before
  public void init() {
    SmartFileCompressionInfo.Builder builder = SmartFileCompressionInfo.newBuilder();
    compressionInfo = builder
        .setFileName(fileName)
        .setBufferSize(bufferSize)
        .setOriginalLength(originalLength)
        .setCompressedLength(compressedLength)
        .setOriginalPos(originalPos)
        .setCompressedPos(compressedPos)
        .build();
  }

  @Test
  public void testBasicVariables() {
    Assert.assertEquals(fileName, compressionInfo.getFileName());
    Assert.assertEquals(bufferSize, compressionInfo.getBufferSize());
    Assert.assertEquals(originalLength, compressionInfo.getOriginalLength());
    Assert.assertEquals(compressedLength, compressionInfo.getCompressedLength());
    Assert.assertArrayEquals(originalPos, compressionInfo.getOriginalPos());
    Assert.assertArrayEquals(compressedPos, compressionInfo.getCompressedPos());
  }

  @Test
  public void testGetPosIndex() {
    int index;
    // Original offset
    index = compressionInfo.getPosIndexByOriginalOffset(-1);
    Assert.assertEquals(0, index);
    index = compressionInfo.getPosIndexByOriginalOffset(0);
    Assert.assertEquals(0, index);
    index = compressionInfo.getPosIndexByOriginalOffset(3);
    Assert.assertEquals(0, index);
    index = compressionInfo.getPosIndexByOriginalOffset(39);
    Assert.assertEquals(3, index);
    index = compressionInfo.getPosIndexByOriginalOffset(50);
    Assert.assertEquals(5, index);
    index = compressionInfo.getPosIndexByOriginalOffset(85);
    Assert.assertEquals(8, index);
    index = compressionInfo.getPosIndexByOriginalOffset(90);
    Assert.assertEquals(8, index);
    // Compressed offset
    index = compressionInfo.getPosIndexByCompressedOffset(-1);
    Assert.assertEquals(0, index);
    index = compressionInfo.getPosIndexByCompressedOffset(0);
    Assert.assertEquals(0, index);
    index = compressionInfo.getPosIndexByCompressedOffset(3);
    Assert.assertEquals(0, index);
    index = compressionInfo.getPosIndexByCompressedOffset(18);
    Assert.assertEquals(3, index);
    index = compressionInfo.getPosIndexByCompressedOffset(22);
    Assert.assertEquals(5, index);
    index = compressionInfo.getPosIndexByCompressedOffset(33);
    Assert.assertEquals(8, index);
    index = compressionInfo.getPosIndexByCompressedOffset(90);
    Assert.assertEquals(8, index);
  }

  @Test
  public void testGetTrunkSize() {
    long trunkSize;
    try {
      // Original trunk size
      trunkSize = compressionInfo.getOriginTrunkSize(1);
      Assert.assertEquals(10, trunkSize);
      trunkSize = compressionInfo.getOriginTrunkSize(8);
      Assert.assertEquals(6, trunkSize);
      // Compressed trunk size
      trunkSize = compressionInfo.getCompressedTrunkSize(1);
      Assert.assertEquals(6, trunkSize);
      trunkSize = compressionInfo.getCompressedTrunkSize(8);
      Assert.assertEquals(2, trunkSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Wrong index
    try {
      trunkSize = compressionInfo.getOriginTrunkSize(-1);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLocateCompressionTrunk() throws IOException {
    CompressionTrunk compressionTrunk;
    // Original offset
    compressionTrunk = compressionInfo.locateCompressionTrunk(false, 12);
    Assert.assertEquals(1, compressionTrunk.getIndex());
    Assert.assertEquals(1L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(4L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(6L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionInfo.locateCompressionTrunk(false, 40);
    Assert.assertEquals(4, compressionTrunk.getIndex());
    Assert.assertEquals(40L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(19L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(3L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionInfo.locateCompressionTrunk(false, 85);
    Assert.assertEquals(8, compressionTrunk.getIndex());
    Assert.assertEquals(80L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(6L, compressionTrunk.getOriginLength());
    Assert.assertEquals(32L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(2L, compressionTrunk.getCompressedLength());

    // Compressed offset
    compressionTrunk = compressionInfo.locateCompressionTrunk(true, 6);
    Assert.assertEquals(1, compressionTrunk.getIndex());
    Assert.assertEquals(10L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(4L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(6L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionInfo.locateCompressionTrunk(true, 19);
    Assert.assertEquals(4, compressionTrunk.getIndex());
    Assert.assertEquals(40L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(19L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(3L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionInfo.locateCompressionTrunk(true, 33);
    Assert.assertEquals(8, compressionTrunk.getIndex());
    Assert.assertEquals(80L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(6L, compressionTrunk.getOriginLength());
    Assert.assertEquals(32L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(2L, compressionTrunk.getCompressedLength());
  }
}
