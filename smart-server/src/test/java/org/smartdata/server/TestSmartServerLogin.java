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
package org.smartdata.server;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.util.NetworkUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.MiniClusterFactory;
import org.smartdata.metastore.TestDBUtil;
import org.smartdata.metastore.utils.MetaStoreUtils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;

/**
 * Test.
 */
public class TestSmartServerLogin {
  private SimpleKdcServer kdcServer;
  private String serverHost = "localhost";
  private int serverPort = -1;
  private SmartConf conf;
  private MiniDFSCluster cluster;
  private String dbFile;
  private String dbUrl;
  private SmartServer ssm;

  private final String keytabFileName = "smart.keytab";
  private final String principal = "ssmroot@EXAMPLE.COM";

  @Before
  public void setupKdcServer() throws Exception {
    kdcServer = new SimpleKdcServer();
    kdcServer.setKdcHost(serverHost);
    kdcServer.setAllowUdp(false);
    kdcServer.setAllowTcp(true);
    serverPort = NetworkUtil.getServerPort();
    kdcServer.setKdcTcpPort(serverPort);
    kdcServer.init();
    kdcServer.start();
  }

  private void initConf() throws Exception {
    conf = new SmartConf();
    cluster = MiniClusterFactory.get().create(3, conf);
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    List<URI> uriList = new ArrayList<>(namenodes);
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());
    conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
        uriList.get(0).toString());

    // Set db used
    dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    dbUrl = MetaStoreUtils.SQLITE_URL_PREFIX + dbFile;
    conf.set(SmartConfKeys.SMART_METASTORE_DB_URL_KEY, dbUrl);

    conf.setBoolean(SmartConfKeys.SMART_SECURITY_ENABLE, true);
    conf.set(SmartConfKeys.SMART_SERVER_KEYTAB_FILE_KEY, keytabFileName);
    conf.set(SmartConfKeys.SMART_SERVER_KERBEROS_PRINCIPAL_KEY, principal);
  }

  private File generateKeytab(String keytabFileName, String principal) throws Exception {
    File keytabFile = new File(keytabFileName);
    kdcServer.createAndExportPrincipals(keytabFile, principal);
    return new File(keytabFileName);
  }

  @Test
  public void loginSmartServerUsingKeytab() throws Exception {
    initConf();
    generateKeytab(keytabFileName, principal);
    ssm = SmartServer.launchWith(conf);
  }

  @After
  public void tearDown() throws Exception {
    File keytabFile = new File(keytabFileName);
    if (keytabFile.exists()) {
      keytabFile.delete();
    }
    if (kdcServer != null) {
      kdcServer.stop();
    }
    if (ssm != null) {
      ssm.shutdown();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
