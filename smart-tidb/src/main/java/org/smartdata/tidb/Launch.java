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

public class Launch implements Runnable {
  private final static Logger LOG = LoggerFactory.getLogger(Launch.class);

  public void run() {
    String pdArgs = new String("--data-dir=pd --log-file=logs/pd.log");
    String tikvArgs = new String("--pd=127.0.0.1:2379 --data-dir=tikv --log-file=logs/tikv.log");
    String tidbArgs = new String("--store=tikv --path=127.0.0.1:2379 --log-file=logs/tidb.log");
    //String tidbArgs = new String("--log-file=logs/tidb.log");

    PdServer pdServer = new PdServer(pdArgs);
    TikvServer tikvServer = new TikvServer(tikvArgs);
    TidbServer tidbServer = new TidbServer(tidbArgs);

    Thread pdThread = new Thread(pdServer);
    pdThread.start();
    try {
      Thread.sleep(4000);
      Thread tikvThread = new Thread(tikvServer);
      tikvThread.start();
      Thread.sleep(6000);
      Thread tidbThread = new Thread(tidbServer);
      tidbThread.start();
    } catch (InterruptedException ex) {
      LOG.error(ex.getMessage());
    }
  }
}
