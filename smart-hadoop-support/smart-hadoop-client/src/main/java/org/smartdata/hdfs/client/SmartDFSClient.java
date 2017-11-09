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
package org.smartdata.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.SmartDFSInputStream;
import org.smartdata.client.SmartClient;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.FileContainerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class SmartDFSClient extends DFSClient {
  private SmartClient smartClient = null;
  private boolean healthy = false;
  private List<String> smallFileList = new ArrayList<>();

  public SmartDFSClient(InetSocketAddress nameNodeAddress, Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeAddress, conf);
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      smallFileList = smartClient.getSmallFileList();
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(URI nameNodeUri, Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeUri, conf);
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      smallFileList = smartClient.getSmallFileList();
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(URI nameNodeUri, Configuration conf,
      FileSystem.Statistics stats, InetSocketAddress smartServerAddress)
      throws IOException {
    super(nameNodeUri, conf, stats);
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      smallFileList = smartClient.getSmallFileList();
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(conf);
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      smallFileList = smartClient.getSmallFileList();
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(Configuration conf) throws IOException {
    super(conf);
    try {
      smartClient = new SmartClient(conf);
      smallFileList = smartClient.getSmallFileList();
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public FileContainerInfo getFileContainerInfo(String src) throws IOException {
    if (!healthy) {
      throw new IOException("smart client is not healthy.");
    }
    return smartClient.getFileContainerInfo(src);
  }

  public List<String> getSmallFileList() throws IOException {
    if (!healthy) {
      throw new IOException("smart client is not healthy.");
    }
    return smartClient.getSmallFileList();
  }

  private boolean isSmallFile(String file) {
    if (smallFileList == null) {
      return false;
    } else {
      return smallFileList.contains(file);
    }
  }

  // TODO: handle small file access event
  @Override
  public DFSInputStream open(String src) throws IOException {
    if (!isSmallFile(src)) {
      return super.open(src);
      //reportFileAccessEvent(src);
    } else {
      return new SmartDFSInputStream(this, src, true, getFileContainerInfo(src));
    }
  }

  @Override
  public DFSInputStream open(String src, int bufferSize, boolean verifyChecksum)
      throws IOException {
    if (!isSmallFile(src)) {
      return super.open(src, bufferSize, verifyChecksum);
      //reportFileAccessEvent(src);
    } else {
      return new SmartDFSInputStream(this, src, true, getFileContainerInfo(src));
    }
  }

  @Deprecated
  @Override
  public DFSInputStream open(String src, int bufferSize,
      boolean verifyChecksum, FileSystem.Statistics stats)
      throws IOException {
    return open(src, bufferSize, verifyChecksum);
  }

  private void reportFileAccessEvent(String src) {
    try {
      if (!healthy) {
        return;
      }
      smartClient.reportFileAccessEvent(new FileAccessEvent(src));
    } catch (IOException e) {
      // Here just ignores that failed to report
      LOG.error("Cannot report file access event to SmartServer: " + src
          + " , for: " + e.getMessage()
          + " , report mechanism will be disabled now in this instance.");
      healthy = false;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      super.close();
    } catch (IOException e) {
      throw e;
    } finally {
      try {
        if (smartClient != null) {
          smartClient.close();
        }
      }finally {
        healthy = false;
      }
    }
  }
}
