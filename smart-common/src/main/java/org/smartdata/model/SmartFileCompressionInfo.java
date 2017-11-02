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
  private List<Integer> originalPos;
  private List<Integer> compressedPos;

  public SmartFileCompressionInfo(String fileName, int bufferSize) {
    this.fileName = fileName;
    this.bufferSize = bufferSize;
    originalPos = new ArrayList<>();
    compressedPos = new ArrayList<>();
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public String getFileName() {
    return fileName;
  }

  public List<Integer> getOriginalPos() {
    return originalPos;
  }

  public List<Integer> getCompressedPos() {
    return compressedPos;
  }

  public void setPositionMapping(int originalPosition, int compressedPosition) {
    originalPos.add(originalPosition);
    compressedPos.add(compressedPosition);
  }
}
