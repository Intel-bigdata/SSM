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

package org.smartdata.server.engine;

public class CopyTargetTask {
  private String dest;
  private String source;
  private long offset;
  private long length;

  public CopyTargetTask(String dest, String source, long offset, long length) {
    this.dest = dest;
    this.source = source;
    this.offset = offset;
    this.length = length;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public String getDest() {
    return dest;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public String getSource() {
    return source;
  }

  @Override
  public String toString() {
    return "CopyTargetTask{" +
        "dest='" + dest + '\'' +
        ", source='" + source + '\'' +
        ", offset=" + offset +
        ", length=" + length +
        '}';
  }
}
