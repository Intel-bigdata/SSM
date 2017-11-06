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
package org.smartdata.integration;

import org.smartdata.SmartServiceState;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.TestDBUtil;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.server.SmartServer;

/**
 * A SmartServer for integration test.
 */
public class IntegrationSmartServer {
  private SmartConf conf;
  private SmartServer ssm;
  private String dbFile;
  private String dbUrl;

  public void setUp(SmartConf conf) throws Exception {
    this.conf = conf;
    // Set db used
    String db = conf.get(SmartConfKeys.SMART_METASTORE_DB_URL_KEY);
    if (db == null || db.length() == 0) {
      dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
      dbUrl = MetaStoreUtils.SQLITE_URL_PREFIX + dbFile;
      conf.set(SmartConfKeys.SMART_METASTORE_DB_URL_KEY, dbUrl);
    }

    ssm = SmartServer.launchWith(conf);
    waitTillSSMExitSafeMode();
  }

  private void waitTillSSMExitSafeMode() throws Exception {
    SmartAdmin client = new SmartAdmin(conf);
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

  public void cleanUp() {
    if (ssm != null) {
      ssm.shutdown();
    }
  }

  public SmartServer getSsm() {
    return ssm;
  }
}
