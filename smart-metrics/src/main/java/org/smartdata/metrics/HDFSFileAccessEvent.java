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
 * An HDFS file access event.
 */
public class HDFSFileAccessEvent implements FileAccessEvent {
  private final String path;
  private final long timeStamp;

  public HDFSFileAccessEvent(String path) {
    this(path, -1);
  }

  public HDFSFileAccessEvent(String path, long timestamp) {
    this.path = path;
    this.timeStamp = timestamp;
  }

  @Override
  public String getPath() {
    return this.path;
  }

  // DFSClient has no info about the file id, except have another rpc call
  // to Namenode, SmartServer can get this value from Namespace, so not
  // provide id info here.
  @Override
  public long getFileId() {
    return 0;
  }

  @Override
  public String getAccessedBy() {
    return null;
  }

  @Override
  public long getTimestamp() {
    return this.timeStamp;
  }
}
