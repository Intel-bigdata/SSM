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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.model.FileInfo;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

/**
 * Contain utils related to hadoop cluster.
 */
public class HadoopUtil {
  public static final String HDFS_CONF_DIR = "hdfs";

  public static final Logger LOG =
      LoggerFactory.getLogger(HadoopUtil.class);

  /**
   * HDFS cluster's configuration should be placed in the following directory:
   *    ${SMART_CONF_DIR_KEY}/hdfs/${namenode host}
   *
   * For example, ${SMART_CONF_DIR_KEY}=/var/ssm/conf, nameNodeUrl="hdfs://nnhost:9000",
   *    then, its config files should be placed in "/var/ssm/conf/hdfs/nnhost".
   *
   * @param ssmConf
   * @param nameNodeUrl
   * @param conf
   */
  public static void loadHadoopConf(SmartConf ssmConf, URL nameNodeUrl, Configuration conf)
      throws IOException {
    String ssmConfDir = ssmConf.get(SmartConfKeys.SMART_CONF_DIR_KEY);
    if (ssmConfDir == null || ssmConfDir.equals("")) {
      return;
    }
    loadHadoopConf(ssmConfDir, nameNodeUrl, conf);
  }

  public static void loadHadoopConf(String ssmConfDir, URL nameNodeUrl, Configuration conf)
      throws IOException {
    if (nameNodeUrl == null || nameNodeUrl.getHost() == null) {
      return;
    }

    String dir;
    if (ssmConfDir.endsWith("/")) {
      dir = ssmConfDir + HDFS_CONF_DIR + "/" + nameNodeUrl.getHost();
    } else {
      dir = ssmConfDir + "/" + HDFS_CONF_DIR + "/" + nameNodeUrl.getHost();
    }
    loadHadoopConf(conf, dir);
  }

  /**
   * Load hadoop configure files in the given directory to 'conf'.
   *
   * @param conf
   * @param hadoopConfPath directory that hadoop config files located.
   */
  public static void loadHadoopConf(Configuration conf, String hadoopConfPath)
      throws IOException {
    if (hadoopConfPath == null || hadoopConfPath.isEmpty()) {
      LOG.info("Hadoop configuration path is not set");
    } else {
      URL hadoopConfDir;
      try {
        if (!hadoopConfPath.endsWith("/")) {
          hadoopConfPath += "/";
        }
        try {
          hadoopConfDir = new URL(hadoopConfPath);
        } catch (MalformedURLException e) {
          hadoopConfDir = new URL("file://" + hadoopConfPath);
        }
        Path hadoopConfDirPath = Paths.get(hadoopConfDir.toURI());
        if (Files.exists(hadoopConfDirPath) &&
            Files.isDirectory(hadoopConfDirPath)) {
          LOG.debug("Hadoop configuration path = " + hadoopConfPath);
        } else {
          throw new IOException("Hadoop configuration path [" + hadoopConfPath
              + "] doesn't exist or is not a directory");
        }

        try {
          URL coreConfFile = new URL(hadoopConfDir, "core-site.xml");
          Path coreFilePath = Paths.get(coreConfFile.toURI());
          if (Files.exists(coreFilePath)) {
            conf.addResource(coreConfFile);
            LOG.debug("Hadoop configuration file [" +
                coreConfFile.toExternalForm() + "] is loaded");
          } else {
            throw new IOException("Hadoop configuration file [" +
                coreConfFile.toExternalForm() + "] doesn't exist");
          }
        } catch (MalformedURLException e1) {
          throw new IOException("Access hadoop configuration file core-site.xml failed", e1);
        }

        try {
          URL hdfsConfFile = new URL(hadoopConfDir, "hdfs-site.xml");
          Path hdfsFilePath = Paths.get(hdfsConfFile.toURI());
          if (Files.exists(hdfsFilePath)) {
            conf.addResource(hdfsConfFile);
            LOG.debug("Hadoop configuration file [" +
                hdfsConfFile.toExternalForm() + "] is loaded");
          } else {
            throw new IOException("Hadoop configuration file [" +
                hdfsConfFile.toExternalForm() + "] doesn't exist");
          }
        } catch (MalformedURLException e1) {
          throw new IOException("Access hadoop configuration file hdfs-site.xml failed", e1);
        }
      } catch (URISyntaxException e) {
        throw new IOException("Access hadoop configuration path [" + hadoopConfPath
            + "] failed" + e);
      }
    }
  }

  public static URI getNameNodeUri(Configuration conf)
      throws IOException {
    String nnRpcAddr = null;

    String[] rpcAddrKeys = {        
        SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        // Keep this last, haven't find a predefined key for this property
        "fs.defaultFS"
    };

    String[] nnRpcAddrs = new String[rpcAddrKeys.length];

    int lastNotNullIdx = 0;
    for (int index = 0; index < rpcAddrKeys.length; index++) {
      nnRpcAddrs[index] = conf.get(rpcAddrKeys[index]);
      LOG.debug("Get namenode URL, key: " + rpcAddrKeys[index] + ", value:" + nnRpcAddrs[index]);
      lastNotNullIdx = nnRpcAddrs[index] == null ? lastNotNullIdx : index;
      nnRpcAddr = nnRpcAddr == null ? nnRpcAddrs[index] : nnRpcAddr;
    }

    if (nnRpcAddr == null || nnRpcAddr.equalsIgnoreCase("file:///")) {
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
      .setBlockReplication(status.getReplication())
      .setBlocksize(status.getBlockSize())
      .setModificationTime(status.getModificationTime())
      .setAccessTime(status.getAccessTime())
      .setPermission(status.getPermission().toShort())
      .setOwner(status.getOwner())
      .setGroup(status.getGroup())
      .setStoragePolicy(status.getStoragePolicy())
      .build();
  }

  public static DFSClient getDFSClient(final URI nnUri, final Configuration conf)
    throws IOException{
    try {
      return UserGroupInformation.getCurrentUser()
          .doAs(new PrivilegedExceptionAction<DFSClient>() {
        @Override
        public DFSClient run() throws Exception {
          return new DFSClient(nnUri, conf);
        }
      });
    } catch (InterruptedException e) {
      LOG.error("Fail to new DFSClient for : " + e.getMessage());
      throw new IOException("Fail to new DFSClient for : " + e.getMessage());
    }
  }

  public static String translateStoragePoliceId2Name(int id) {
    switch (id){
      case 15:
        return "Lazy_Persist";
      case 12:
        return "All_SSD";
      case 10:
        return "One_SSD";
      case 7:
        return "Hot";
      case 5:
        return "Warm";
      case 2:
        return "Cold";
    }
    return "";
  }

  public static byte translateStoragePoliceName2Id(String name) {
    switch (name){
      case "Lazy_Persist":
        return 15;
      case "All_SSD":
        return 12;
      case "One_SSD":
        return 10;
      case "Hot":
        return 7;
      case "Warm":
        return 5;
      case "Cold":
        return 2;
    }
    return 0;
  }

}
