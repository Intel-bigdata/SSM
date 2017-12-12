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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A class to maintain info of compressed files.
 */
public class SmartFileCompressionInfo {

  private String fileName;
  private int bufferSize;
  private long originalLength;
  private long compressedLength;
  private Long[] originalPos;
  private Long[] compressedPos;

  public SmartFileCompressionInfo(String fileName, int bufferSize) {
    this(fileName, bufferSize, 0, 0, new Long[0], new Long[0]);
  }

  public SmartFileCompressionInfo(String fileName, int bufferSize,
      Long[] originalPos, Long[] compressedPos) {
    this(fileName, bufferSize, 0, 0, originalPos, compressedPos);
  }

  public SmartFileCompressionInfo(String fileName, int bufferSize,
      long originalLength, long compressedLength, Long[] originalPos,
      Long[] compressedPos) {
    this.fileName = fileName;
    this.bufferSize = bufferSize;
    this.originalLength = originalLength;
    this.compressedLength = compressedLength;
    this.originalPos = originalPos;
    this.compressedPos = compressedPos;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public String getFileName() {
    return fileName;
  }

  public long getOriginalLength() {
    return originalLength;
  }

  public long getCompressedLength() {
    return compressedLength;
  }

  public void setOriginalLength(long length) {
    originalLength = length;
  }

  public void setCompressedLength(long length) {
    compressedLength = length;
  }

  public Long[] getOriginalPos() {
    return originalPos;
  }

  public Long[] getCompressedPos() {
    return compressedPos;
  }

  /**
   * Get the index of originalPos and compressedPos of the given original offset.
   *
   * @param offset the offset of original file
   * @return the index of the compression trunk where the offset locates
   */
  public int getPosIndexByOriginalOffset(long offset) {
    int trunkIndex = Arrays.binarySearch(originalPos, offset);
    if (trunkIndex < -1) {
      trunkIndex = -trunkIndex - 2;
    } else if (trunkIndex == -1) {
      trunkIndex = 0;
    }
    return trunkIndex;
  }

  /**
   * Get the index of originalPos and compressedPos of the given compressed offset.
   *
   * @param offset the offset of compressed file
   * @return the index of the compression trunk where the offset locates
   */
  public int getPosIndexByCompressedOffset(long offset) {
    int trunkIndex = Arrays.binarySearch(compressedPos, offset);
    if (trunkIndex < -1) {
      trunkIndex = -trunkIndex - 2;
    } else if (trunkIndex == -1) {
      trunkIndex = 0;
    }
    return trunkIndex;
  }

  /**
   * Locate the compression trunk with the given offset (either origin or compressed).
   *
   * @param compressed true for compressed offset, false for origin offset
   * @param offset
   * @return the compression trunk where the offset locates
   * @throws IOException
   */
  public CompressionTrunk locateCompressionTrunk(boolean compressed,
      long offset) throws IOException {
    int index = compressed ? getPosIndexByCompressedOffset(offset) :
        getPosIndexByOriginalOffset(offset);
    CompressionTrunk compressionTrunk = new CompressionTrunk(index);
    compressionTrunk.setOriginOffset(originalPos[index]);
    compressionTrunk.setOriginLength(getOriginTrunkSize(index));
    compressionTrunk.setCompressedOffset(compressedPos[index]);
    compressionTrunk.setCompressedLength(getCompressedTrunkSize(index));
    return compressionTrunk;
  }

  /**
   * Get original trunk size with the given index.
   *
   * @param index
   * @return
   */
  public long getOriginTrunkSize(int index) throws IOException {
    if (index >= originalPos.length || index < 0) {
      throw new IOException("Trunk index out of bound");
    }
    long trunkSize = 0;
    if (index == originalPos.length - 1) {
      trunkSize = originalLength - originalPos[index];
    } else {
      trunkSize = originalPos[index + 1] - originalPos[index];
    }
    return trunkSize;
  }

  /**
   * Get the compressed trunk size with the given index.
   *
   * @param index
   * @return
   */
  public long getCompressedTrunkSize(int index) throws IOException {
    if (index >= compressedPos.length || index < 0) {
      throw new IOException("Trunk index out of bound");
    }
    long trunkSize = 0;
    if (index == compressedPos.length - 1) {
      trunkSize = compressedLength - compressedPos[index];
    } else {
      trunkSize = compressedPos[index + 1] - compressedPos[index];
    }
    return trunkSize;
  }

  public void setPositionMapping(Long[] originalPos, Long[] compressedPos)
      throws IOException{
    if (originalPos.length != compressedPos.length) {
      throw new IOException("Input of position mapping is incorrect : "
          + "originalPos.length != compressedPos.length");
    }
    this.originalPos = originalPos;
    this.compressedPos = compressedPos;
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static class Builder {
    private String fileName = null;
    private int bufferSize = 0;
    private long originalLength;
    private long compressedLength;
    private Long[] originalPos;
    private Long[] compressedPos;

    public static Builder create() {
      return new Builder();
    }

    public Builder setFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public Builder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder setOriginalLength(long originalLength) {
      this.originalLength = originalLength;
      return this;
    }

    public Builder setCompressedLength(long compressedLength) {
      this.compressedLength = compressedLength;
      return this;
    }

    public Builder setOriginalPos(Long[] originalPos) {
      this.originalPos = originalPos;
      return this;
    }

    public Builder setOriginalPos(List<Long> originalPos) {
      this.originalPos = originalPos.toArray(new Long[0]);
      return this;
    }

    public Builder setCompressedPos(Long[] compressedPos) {
      this.compressedPos = compressedPos;
      return this;
    }

    public Builder setCompressedPos(List<Long> compressedPos) {
      this.compressedPos = compressedPos.toArray(new Long[0]);
      return this;
    }

    public SmartFileCompressionInfo build() {
      return new SmartFileCompressionInfo(fileName, bufferSize, originalLength,
          compressedLength, originalPos, compressedPos);
    }
  }
}
