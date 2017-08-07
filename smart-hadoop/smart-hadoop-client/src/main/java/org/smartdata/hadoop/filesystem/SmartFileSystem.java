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
package org.smartdata.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.smartdata.client.SmartClient;
import org.smartdata.metrics.FileAccessEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

/**
 * SmartFileSystem Deploy Guide
 * 1. Build SSM, get all jar files start with name Smart*
 * 2. Copy these jar files to HDFS classpath
 * 3. Reconfigure HDFS
 *   Please do the following configurations,
 *   1. core-site.xml
 *   Change property "fs.hdfs.impl" value, to point to the Smart Server provided
 *   "Smart  File System".
 *    <property>
 *      <name>fs.hdfs.impl</name>
 *      <value>org.smartdata.filesystem.SmartFileSystem</value>
 *      <description>The FileSystem for hdfs URL</description>
 *    </property>
 *    2. hdfs-site.xml
 *    Add property "smart.server.rpc.adddress" and "smart.server.rpc.port" to
 *    point to installed Smart Server.
 *    <property>
 *      <name>smart.server.rpc.address</name>
 *      <value>127.0.0.1</value>
 *    </property>
 *    <property>
 *      <name>smart.server.rpc.port</name>
 *      <value>7042</value>
 *    </property>
 * 4. Restart HDFS
 */

public class SmartFileSystem extends DistributedFileSystem {

  private SmartClient smartClient;
  private InetSocketAddress smartServerAddress;
  private boolean healthy;

  public SmartFileSystem() {
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    String smartServerIp = conf.get("smart.server.rpc.address", "127.0.0.1");
    int smartServerPort = conf.getInt("smart.server.rpc.port", 7042);

    try {
      smartServerAddress = new InetSocketAddress(smartServerIp, smartServerPort);
    } catch (Exception e){
      try {
        super.close();
      } catch (Throwable e1) {
        // DO nothing now
      }
      throw new IOException("Cannot parse smart server rpc address or port" +
          ", address: " + smartServerIp + ", port:" +  smartServerPort);
    }
    this.smartClient = new SmartClient(conf, smartServerAddress);
    healthy = true;
  }

  @Override
  public FSDataInputStream open(Path file, final int bufferSize)
      throws IOException {
    FSDataInputStream inputStream = super.open(file, bufferSize);
    this.statistics.incrementReadOps(1);
    reportFileAccessEvent(file.toUri().getPath());
    return inputStream;
  }

  private void reportFileAccessEvent(String src) {
    try {
      // If SSM server is down, reduce the impact to upper layer application.
      if (!healthy) {
        return;
      }
      smartClient.reportFileAccessEvent(new FileAccessEvent(src));
    } catch (IOException e) {
      // Here just ignores that failed to report
      healthy = false;
      LOG.error("Cannot report file access event to SmartServer: " + src
          + " , for: " + e.getMessage()
          + " , report mechanism will be disabled now in this instance.");
    }
  }

  public void close() throws IOException {
    try {
      super.close();
    } catch (IOException e) {
      throw e;
    } finally {
      try {
        if (smartClient != null) {
          this.smartClient.close();
        }
      } finally {
        healthy = false;
      }
    }
  }
}
