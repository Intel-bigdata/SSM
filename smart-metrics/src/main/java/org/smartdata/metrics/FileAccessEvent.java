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
package org.smartdata.metrics;

/**
 * A file access event.
 */
public class FileAccessEvent implements DataAccessEvent {
  private final String path;
  private final String user;
  private long timeStamp;

  public FileAccessEvent(String path) {
    this(path, -1);
  }

  public FileAccessEvent(String path, long timestamp) {
    this(path, timestamp, "");
  }

  public FileAccessEvent(String path, long timeStamp, String user) {
    this.path = path;
    this.timeStamp = timeStamp;
    this.user = user;
  }

  public FileAccessEvent(String path, String user) {
    this(path, -1, user);
  }

  /**
   * Get the accessed file path.
   * @return file path
   */
  public String getPath() {
    return this.path;
  }

  // DFSClient has no info about the file id, except have another rpc call
  // to Namenode, SmartServer can get this value from Namespace, so not
  // provide id info here.
  public long getFileId() {
    return 0;
  }

  @Override
  public String getAccessedBy() {
    return this.user;
  }

  @Override
  public long getTimestamp() {
    return this.timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }
}
