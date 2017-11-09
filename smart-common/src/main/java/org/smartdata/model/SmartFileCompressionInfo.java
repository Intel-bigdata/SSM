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

import java.util.ArrayList;
import java.util.List;

/**
 * A class to maintain info of compressed files.
 */
public class SmartFileCompressionInfo {

  private String fileName;
  private int bufferSize;
  private List<Long> originalPos;
  private List<Long> compressedPos;

  public SmartFileCompressionInfo(String fileName, int bufferSize) {
    this(fileName, bufferSize, new ArrayList<Long>(), new ArrayList<Long>());
  }

  public SmartFileCompressionInfo(String fileName, int bufferSize,
      List<Long> originalPos, List<Long> compressedPos) {
    this.fileName = fileName;
    this.bufferSize = bufferSize;
    this.originalPos = originalPos;
    this.compressedPos = compressedPos;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public String getFileName() {
    return fileName;
  }

  public List<Long> getOriginalPos() {
    return originalPos;
  }

  public List<Long> getCompressedPos() {
    return compressedPos;
  }

  public void setPositionMapping(long originalPosition, long compressedPosition) {
    originalPos.add(originalPosition);
    compressedPos.add(compressedPosition);
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static class Builder {
    private String fileName = null;
    private int bufferSize = 0;
    private List<Long> originalPos = new ArrayList<>();
    private List<Long> compressedPos = new ArrayList<>();

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

    public Builder setOriginalPos(List<Long> originalPos) {
      this.originalPos = originalPos;
      return this;
    }

    public Builder setCompressedPos(List<Long> compressedPos) {
      this.compressedPos = compressedPos;
      return this;
    }

    public SmartFileCompressionInfo build() {
      return new SmartFileCompressionInfo(fileName, bufferSize, originalPos,
          compressedPos);
    }
  }
}
