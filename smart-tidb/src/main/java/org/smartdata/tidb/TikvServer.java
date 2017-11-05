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

public class TikvServer implements Runnable {
  private final static Logger LOG = LoggerFactory.getLogger(TikvServer.class);
  private String args;
  private Tikv tikv;

  public interface Tikv extends Library {
    void startServer(String args);

    boolean isTikvServerReady();
  }

  public TikvServer(String args, SmartConf conf) {
    String logDir = conf.get(SmartConfKeys.SMART_LOG_DIR_KEY, SmartConfKeys.SMART_LOG_DIR_DEFAULT);
    this.args = args + " --log-file=" + logDir + "/tikv.log";
    try {
      tikv = (Tikv) Native.loadLibrary("libtikv.so", Tikv.class);
    } catch (UnsatisfiedLinkError ex) {
      LOG.error("libtikv.so can not be found or loaded!");
    }
  }

  public boolean isReady() {
    return tikv.isTikvServerReady();
  }

  public void run() {
    StringBuffer options = new StringBuffer();
    //According to start.rs in our tikv source code, "TiKV" is the flag name used for parsing
    options.append("TiKV");
    options.append(" ");
    options.append(args);

    LOG.info("Starting Tikv..");
    tikv.startServer(options.toString());
  }
}
