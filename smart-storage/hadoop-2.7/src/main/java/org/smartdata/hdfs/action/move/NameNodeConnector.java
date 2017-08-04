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
package org.smartdata.hdfs.action.move;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The class provides utilities for accessing a NameNode.
 */
public class NameNodeConnector implements Closeable {
  private static final Log LOG = LogFactory.getLog(NameNodeConnector.class);

  private final URI nameNodeUri;
  private final String blockpoolID;

  private final NamenodeProtocol namenode;
  private final ClientProtocol client;
  private final KeyManager keyManager;
  final AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);

  private final DistributedFileSystem fs;

  public NameNodeConnector(URI nameNodeUri, Configuration conf)
      throws IOException {
    this.nameNodeUri = nameNodeUri;

    this.namenode = NameNodeProxies.createProxy(conf, nameNodeUri,
        NamenodeProtocol.class).getProxy();
    this.client = NameNodeProxies.createProxy(conf, nameNodeUri,
        ClientProtocol.class, fallbackToSimpleAuth).getProxy();
    this.fs = (DistributedFileSystem) FileSystem.get(nameNodeUri, conf);

    final NamespaceInfo namespaceinfo = namenode.versionRequest();
    this.blockpoolID = namespaceinfo.getBlockPoolID();

    final FsServerDefaults defaults = fs.getServerDefaults(new Path("/"));
    this.keyManager = new KeyManager(blockpoolID, namenode,
        defaults.getEncryptDataTransfer(), conf);
  }

  public DistributedFileSystem getDistributedFileSystem() {
    return fs;
  }

  /** @return the block pool ID */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  /** @return blocks with locations. */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
      throws IOException {
    return namenode.getBlocks(datanode, size);
  }

  /** @return live datanode storage reports. */
  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {
    return client.getDatanodeStorageReport(DatanodeReportType.LIVE);
  }

  /** @return the key manager */
  public KeyManager getKeyManager() {
    return keyManager;
  }

  @Override
  public void close() {
    keyManager.close();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[namenodeUri=" + nameNodeUri
        + ", bpid=" + blockpoolID + "]";
  }
}
