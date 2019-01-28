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

public class TestCompressionFileState {
  private CompressionFileState compressionFileState;
  private final String fileName = "/testFile";
  private final int bufferSize = 10;
  private final String compressionImpl = "snappy";
  private final long originalLength = 86;
  private final long compressedLength = 34;
  private final Long[] originalPos = {0L, 10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L};
  private final Long[] compressedPos = {0L, 4L, 10L, 13L, 19L, 22L, 25L, 28L, 32L};


  @Before
  public void init() {
    CompressionFileState.Builder builder = CompressionFileState.newBuilder();
    compressionFileState = builder
        .setFileStage(FileState.FileStage.DONE)
        .setFileName(fileName)
        .setBufferSize(bufferSize)
        .setCompressImpl(compressionImpl)
        .setOriginalLength(originalLength)
        .setCompressedLength(compressedLength)
        .setOriginalPos(originalPos)
        .setCompressedPos(compressedPos)
        .build();
  }

  @Test
  public void testBasicVariables() {
    Assert.assertEquals(FileState.FileStage.DONE, compressionFileState.getFileStage());
    Assert.assertEquals(fileName, compressionFileState.getPath());
    Assert.assertEquals(bufferSize, compressionFileState.getBufferSize());
    Assert.assertEquals(compressionImpl, compressionFileState.getCompressionImpl());
    Assert.assertEquals(originalLength, compressionFileState.getOriginalLength());
    Assert.assertEquals(compressedLength, compressionFileState.getCompressedLength());
    Assert.assertArrayEquals(originalPos, compressionFileState.getOriginalPos());
    Assert.assertArrayEquals(compressedPos, compressionFileState.getCompressedPos());
  }

  @Test
  public void testGetPosIndex() {
    int index;
    // Original offset
    index = compressionFileState.getPosIndexByOriginalOffset(-1);
    Assert.assertEquals(0, index);
    index = compressionFileState.getPosIndexByOriginalOffset(0);
    Assert.assertEquals(0, index);
    index = compressionFileState.getPosIndexByOriginalOffset(3);
    Assert.assertEquals(0, index);
    index = compressionFileState.getPosIndexByOriginalOffset(39);
    Assert.assertEquals(3, index);
    index = compressionFileState.getPosIndexByOriginalOffset(50);
    Assert.assertEquals(5, index);
    index = compressionFileState.getPosIndexByOriginalOffset(85);
    Assert.assertEquals(8, index);
    index = compressionFileState.getPosIndexByOriginalOffset(90);
    Assert.assertEquals(8, index);
    // Compressed offset
    index = compressionFileState.getPosIndexByCompressedOffset(-1);
    Assert.assertEquals(0, index);
    index = compressionFileState.getPosIndexByCompressedOffset(0);
    Assert.assertEquals(0, index);
    index = compressionFileState.getPosIndexByCompressedOffset(3);
    Assert.assertEquals(0, index);
    index = compressionFileState.getPosIndexByCompressedOffset(18);
    Assert.assertEquals(3, index);
    index = compressionFileState.getPosIndexByCompressedOffset(22);
    Assert.assertEquals(5, index);
    index = compressionFileState.getPosIndexByCompressedOffset(33);
    Assert.assertEquals(8, index);
    index = compressionFileState.getPosIndexByCompressedOffset(90);
    Assert.assertEquals(8, index);
  }

  @Test
  public void testGetTrunkSize() {
    long trunkSize;
    try {
      // Original trunk size
      trunkSize = compressionFileState.getOriginTrunkSize(1);
      Assert.assertEquals(10, trunkSize);
      trunkSize = compressionFileState.getOriginTrunkSize(8);
      Assert.assertEquals(6, trunkSize);
      // Compressed trunk size
      trunkSize = compressionFileState.getCompressedTrunkSize(1);
      Assert.assertEquals(6, trunkSize);
      trunkSize = compressionFileState.getCompressedTrunkSize(8);
      Assert.assertEquals(2, trunkSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Wrong index
    try {
      trunkSize = compressionFileState.getOriginTrunkSize(-1);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLocateCompressionTrunk() throws IOException {
    CompressionTrunk compressionTrunk;
    // Original offset
    compressionTrunk = compressionFileState.locateCompressionTrunk(false, 12);
    Assert.assertEquals(1, compressionTrunk.getIndex());
    Assert.assertEquals(10L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(4L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(6L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionFileState.locateCompressionTrunk(false, 40);
    Assert.assertEquals(4, compressionTrunk.getIndex());
    Assert.assertEquals(40L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(19L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(3L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionFileState.locateCompressionTrunk(false, 85);
    Assert.assertEquals(8, compressionTrunk.getIndex());
    Assert.assertEquals(80L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(6L, compressionTrunk.getOriginLength());
    Assert.assertEquals(32L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(2L, compressionTrunk.getCompressedLength());

    // Compressed offset
    compressionTrunk = compressionFileState.locateCompressionTrunk(true, 6);
    Assert.assertEquals(1, compressionTrunk.getIndex());
    Assert.assertEquals(10L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(4L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(6L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionFileState.locateCompressionTrunk(true, 19);
    Assert.assertEquals(4, compressionTrunk.getIndex());
    Assert.assertEquals(40L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(10L, compressionTrunk.getOriginLength());
    Assert.assertEquals(19L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(3L, compressionTrunk.getCompressedLength());
    compressionTrunk = compressionFileState.locateCompressionTrunk(true, 33);
    Assert.assertEquals(8, compressionTrunk.getIndex());
    Assert.assertEquals(80L, compressionTrunk.getOriginOffset());
    Assert.assertEquals(6L, compressionTrunk.getOriginLength());
    Assert.assertEquals(32L, compressionTrunk.getCompressedOffset());
    Assert.assertEquals(2L, compressionTrunk.getCompressedLength());
  }
}
