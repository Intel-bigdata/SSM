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
package org.smartdata.tidb;

import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import com.sun.jna.Library;
import com.sun.jna.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TidbServer implements Runnable {
  private final static Logger LOG = LoggerFactory.getLogger(TidbServer.class);
  private String args;
  private Tidb tidb;

  public interface Tidb extends Library {
    void startServer(String args);

    boolean isTidbServerReady();
  }

  public TidbServer(SmartConf conf) {
    this("127.0.0.1", conf);
  }

  public TidbServer(String pdHost, SmartConf conf) {
    String pdPort = conf.get(SmartConfKeys.PD_CLIENT_PORT_KEY, SmartConfKeys.PD_CLIENT_PORT_DEFAULT);
    String servePort = conf.get(SmartConfKeys.TIDB_SERVICE_PORT_KEY, SmartConfKeys.TIDB_SERVICE_PORT_KEY_DEFAULT);
    String logDir = conf.get(SmartConfKeys.SMART_LOG_DIR_KEY, SmartConfKeys.SMART_LOG_DIR_DEFAULT);
    //The default lease time is 10s. Setting a smaller value may decrease the time of executing ddl statement, but it's dangerous to do so.
    args = String.format("--store=tikv --path=%s:%s --P=%s --lease=10s --log-file=%s/tidb.log",
            pdHost, pdPort, servePort, logDir);

    try {
      tidb = (Tidb) Native.loadLibrary("libtidb.so", Tidb.class);
    } catch (UnsatisfiedLinkError ex) {
      LOG.error("libtidb.so can not be found or loaded!");
    }
  }

  public boolean isReady() {
    return tidb.isTidbServerReady();
  }

  public void run() {
    LOG.info("Starting Tidb..");
    tidb.startServer(args);
  }
}