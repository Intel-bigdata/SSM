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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.client.SmartClient;
import org.smartdata.metrics.FileAccessEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

public class SmartDFSClient extends DFSClient {
  private static final Logger LOG = LoggerFactory.getLogger(SmartDFSClient.class);
  private SmartClient smartClient = null;
  private boolean healthy = false;

  public SmartDFSClient(InetSocketAddress nameNodeAddress, Configuration conf,
      InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeAddress, conf);
    try {
      smartClient = new SmartClient(conf, smartServerAddress);
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  public SmartDFSClient(final URI nameNodeUri, final Configuration conf,
      final InetSocketAddress smartServerAddress) throws IOException {
    super(nameNodeUri, conf);

    try {
      smartClient = new SmartClient(conf, smartServerAddress);
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
      healthy = true;
    } catch (IOException e) {
      super.close();
      throw e;
    }
  }

  @Override
  public DFSInputStream open(String src)
      throws IOException, UnresolvedLinkException {
    return super.open(src);
  }

  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    DFSInputStream is = super.open(src, buffersize, verifyChecksum);
    reportFileAccessEvent(src);
    return is;
  }

  @Deprecated
  @Override
  public DFSInputStream open(String src, int buffersize,
      boolean verifyChecksum, FileSystem.Statistics stats)
      throws IOException, UnresolvedLinkException {
    return super.open(src, buffersize, verifyChecksum, stats);
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
