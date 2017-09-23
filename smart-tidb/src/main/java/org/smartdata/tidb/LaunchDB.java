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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LaunchDB implements Runnable {
  private PdServer pdServer;
  private TikvServer tikvServer;
  private TidbServer tidbServer;
  private boolean dbReady = false;
  private final static Logger LOG = LoggerFactory.getLogger(LaunchDB.class);

  public LaunchDB() {
    String pdArgs = new String("--data-dir=pd --log-file=logs/pd.log");
    String tikvArgs = new String("--pd=127.0.0.1:2379 --data-dir=tikv --log-file=logs/tikv.log");
    //The default lease time is 10s. Setting a smaller value may decrease the time of executing ddl statement, but it's dangerous to do so.
    String tidbArgs = new String("--store=tikv --path=127.0.0.1:2379 --lease=1s --log-file=logs/tidb.log");
    //String tidbArgs = new String("--log-file=logs/tidb.log");

    pdServer = new PdServer(pdArgs);
    tikvServer = new TikvServer(tikvArgs);
    tidbServer = new TidbServer(tidbArgs);
  }

  public boolean isCompleted() {
    return dbReady;
  }

  public void run() {
    Thread pdThread = new Thread(pdServer);
    pdThread.start();
    try {
      while (!pdServer.isReady()) {
        Thread.sleep(100);
      }
      LOG.info("Pd server is ready.");

      Thread tikvThread = new Thread(tikvServer);
      tikvThread.start();
      while (!tikvServer.isReady()) {
        Thread.sleep(100);
      }
      LOG.info("Tikv server is ready.");

      Thread tidbThread = new Thread(tidbServer);
      tidbThread.start();
      while (!tidbServer.isReady()) {
        Thread.sleep(100);
      }
      LOG.info("Tidb server is ready.");
      dbReady = true;
    } catch (InterruptedException ex) {
      LOG.error(ex.getMessage());
    }
  }
}
