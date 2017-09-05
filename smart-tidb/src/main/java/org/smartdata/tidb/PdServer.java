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

import com.sun.jna.Library;
import com.sun.jna.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PdServer implements Runnable {
  private String args;
  private final static Logger LOG = LoggerFactory.getLogger(PdServer.class);

  public interface Pd extends Library {
    void startServer(String args);
  }

  public PdServer(String args) {
    this.args = args;
  }

  public void run() {
    Pd pd = null;
    try {
      pd = (Pd) Native.loadLibrary("libpd.so", Pd.class);
    } catch (UnsatisfiedLinkError ex) {
      LOG.error(ex.getMessage());
    }

    LOG.info("Starting PD..");
    pd.startServer(args);
  }
}
