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
import org.junit.After;
import org.junit.Before;
import org.smartdata.SmartServiceState;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.MiniClusterWithStoragesHarness;
import org.smartdata.metastore.TestDBUtil;
import org.smartdata.metastore.utils.MetaStoreUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;

public class MiniSmartClusterHarness extends MiniClusterWithStoragesHarness {
  protected SmartServer ssm;
  private String dbFile;
  private String dbUrl;

  @Before
  @Override
  public void init() throws Exception {
    super.init();
    // Set db used
    SmartConf conf = smartContext.getConf();
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    List<URI> uriList = new ArrayList<>(namenodes);
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, uriList.get(0).toString());
    conf.set(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY,
      uriList.get(0).toString());

    dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    dbUrl = MetaStoreUtils.SQLITE_URL_PREFIX + dbFile;
    smartContext.getConf().set(SmartConfKeys.SMART_METASTORE_DB_URL_KEY, dbUrl);

    // rpcServer start in SmartServer
    ssm = SmartServer.launchWith(conf);
  }

  public void waitTillSSMExitSafeMode() throws Exception {
    SmartAdmin client = new SmartAdmin(smartContext.getConf());
    long start = System.currentTimeMillis();
    int retry = 5;
    while (true) {
      try {
        SmartServiceState state = client.getServiceState();
        if (state != SmartServiceState.SAFEMODE) {
          break;
        }
        int secs = (int) (System.currentTimeMillis() - start) / 1000;
        System.out.println("Waited for " + secs + " seconds ...");
        Thread.sleep(1000);
      } catch (Exception e) {
        if (retry <= 0) {
          throw e;
        }
        retry--;
      }
    }
  }

  @After
  @Override
  public void shutdown() throws IOException {
    if (ssm != null) {
      ssm.shutdown();
    }
    super.shutdown();
  }
}
