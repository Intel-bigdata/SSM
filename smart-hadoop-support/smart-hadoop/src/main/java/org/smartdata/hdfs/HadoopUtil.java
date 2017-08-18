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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.FileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Contain utils related to hadoop cluster.
 */
public class HadoopUtil {

  public static final Logger LOG =
      LoggerFactory.getLogger(HadoopUtil.class);

  public static void loadHadoopConf(Configuration conf) {

    String hadoopConfPath = conf.get(SmartConfKeys.SMART_HADOOP_CONF_PATH_KEY);
    if (hadoopConfPath == null || hadoopConfPath.isEmpty()) {
      LOG.info("Hadoop configuration path is not set");
    } else {
      URL hadoopConfDir;
      try {
        if (hadoopConfPath.endsWith("/")) {
          hadoopConfPath += "/";
        }
        hadoopConfDir = new URL(hadoopConfPath);
        Path hadoopConfDirPath = Paths.get(hadoopConfDir.toURI());
        if (Files.exists(hadoopConfDirPath) &&
            Files.isDirectory(hadoopConfDirPath)) {
          LOG.info("Hadoop configuration path = " + hadoopConfPath);
        } else {
          LOG.error("Hadoop configuration path [" + hadoopConfPath
              + "] doesn't exist or is not a directory");
          return;
        }

        try {
          URL coreConfFile = new URL(hadoopConfDir, "core-site.xml");
          Path coreFilePath = Paths.get(coreConfFile.toURI());
          if (Files.exists(coreFilePath)) {
            conf.addResource(coreConfFile);
            LOG.info("Hadoop configuration file [" +
                coreConfFile.toExternalForm() + "] is loaded");
          } else {
            LOG.error("Hadoop configuration file [" +
                coreConfFile.toExternalForm() + "] doesn't exist");
          }
        } catch (MalformedURLException e1) {
          LOG.error("Access hadoop configuration file core-site.xml failed " +
              "for: " + e1.getMessage());
        }

        try {
          URL hdfsConfFile = new URL(hadoopConfDir, "hdfs-site.xml");
          Path hdfsFilePath = Paths.get(hdfsConfFile.toURI());
          if (Files.exists(hdfsFilePath)) {
            conf.addResource(hdfsConfFile);
            LOG.info("Hadoop configuration file [" +
                hdfsConfFile.toExternalForm() + "] is loaded");
          } else {
            LOG.error("Hadoop configuration file [" +
                hdfsConfFile.toExternalForm() + "] doesn't exist");
          }
        } catch (MalformedURLException e1) {
          LOG.error("Access hadoop configuration file hdfs-site.xml failed " +
              "for: " + e1.getMessage());
        }
      } catch (MalformedURLException | URISyntaxException e) {
        LOG.error("Access hadoop configuration path [" + hadoopConfPath
            + "] failed for: " + e.getMessage());
      }
    }
  }

  public static URI getNameNodeUri(Configuration conf)
      throws IOException {
    String nnRpcAddr = null;

    String[] rpcAddrKeys = {
        // Keep this first, haven't find a predefined key for this property
        "fs.defaultFS",
        SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY
    };

    String[] nnRpcAddrs = new String[rpcAddrKeys.length];

    int lastNotNullIdx = 0;
    for (int index = 0; index < rpcAddrKeys.length; index++) {
      nnRpcAddrs[index] = conf.get(rpcAddrKeys[index]);
      LOG.debug("Get namenode URL, key: " + rpcAddrKeys[index] + ", value:" + nnRpcAddrs[index]);
      lastNotNullIdx = nnRpcAddrs[index] == null ? lastNotNullIdx : index;
      nnRpcAddr = nnRpcAddr == null ? nnRpcAddrs[index] : nnRpcAddr;
    }

    if (nnRpcAddr == null) {
      throw new IOException("Can not find NameNode RPC server address. "
          + "Please configure it through '"
          + SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY + "'.");
    }

    if (lastNotNullIdx == 0 && rpcAddrKeys.length > 1) {
      conf.set(rpcAddrKeys[1], nnRpcAddr);
    }

    try {
      return new URI(nnRpcAddr);
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI Syntax: " + nnRpcAddr, e);
    }
  }

  public static FileInfo convertFileStatus(HdfsFileStatus status, String path) {
    return FileInfo.newBuilder()
      .setPath(path)
      .setFileId(status.getFileId())
      .setLength(status.getLen())
      .setIsdir(status.isDir())
      .setBlock_replication(status.getReplication())
      .setBlocksize(status.getBlockSize())
      .setModification_time(status.getModificationTime())
      .setAccess_time(status.getAccessTime())
      .setPermission(status.getPermission().toShort())
      .setOwner(status.getOwner())
      .setGroup(status.getGroup())
      .setStoragePolicy(status.getStoragePolicy())
      .build();
  }
}
