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

/**
 * CompressionTrunk is the unit of file compression.
 */
public class CompressionTrunk {
  private int index;
  private long originOffset;
  private long originLength;
  private long compressedOffset;
  private long compressedLength;

  public CompressionTrunk(int index) {
    this(index, 0, 0, 0, 0);
  }

  public CompressionTrunk(int index, long originOffset, long originLength,
      long compressedOffset, long compressedLength) {
    this.index = index;
    this.originOffset = originOffset;
    this.originLength = originLength;
    this.compressedOffset = compressedOffset;
    this.compressedLength = compressedLength;
  }

  public void setOriginOffset(long originOffset) {
    this.originOffset = originOffset;
  }

  public void setOriginLength(long originLength) {
    this.originLength = originLength;
  }

  public void setCompressedOffset(long compressedOffset) {
    this.compressedOffset = compressedOffset;
  }

  public void setCompressedLength(long compressedLength) {
    this.compressedLength = compressedLength;
  }

  public int getIndex() {
    return index;
  }

  public long getOriginOffset() {
    return originOffset;
  }

  public long getOriginLength() {
    return originLength;
  }

  public long getCompressedOffset() {
    return compressedOffset;
  }

  public long getCompressedLength() {
    return compressedLength;
  }
}
