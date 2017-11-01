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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton class to maintain info of compressed files.
 */
public class SmartFileCompressionInfo {
  private static SmartFileCompressionInfo instance = new SmartFileCompressionInfo();

  Map<String, CompressedFileMeta> fileTable;

  private SmartFileCompressionInfo() {
    fileTable = new ConcurrentHashMap<>();
  }

  public static SmartFileCompressionInfo getInstance() {
    return instance;
  }

  public void addFile(String fileName, int bufferSize) {
    if (fileTable.containsKey(fileName)) {
      throw new RuntimeException("File " + fileName + " is already compressed");
    }
    fileTable.put(fileName, new CompressedFileMeta(bufferSize));
  }

  public void removeFile(String fileName) {
    fileTable.remove(fileName);
  }

  public boolean compressed(String fileName) {
    return fileTable.containsKey(fileName);
  }

  public int getBufferSize(String fileName) {
    if (!compressed(fileName)) {
      throw new RuntimeException("File " + fileName + " is not compressed");
    }
    return fileTable.get(fileName).getBufferSize();
  }

  class CompressedFileMeta {
    private int bufferSize;

    public CompressedFileMeta(int bufferSize) {
      this.bufferSize = bufferSize;
    }

    public int getBufferSize() {
      return bufferSize;
    }
  }
}
