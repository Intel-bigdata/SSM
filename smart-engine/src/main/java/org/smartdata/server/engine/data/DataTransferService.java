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
package org.smartdata.server.engine.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.server.engine.data.net.TcpPeerServer;

import java.net.InetSocketAddress;

public class DataTransferService {
  private SmartConf conf;
  private ThreadGroup threadGroup;
  private DataXceiverServer xServer;
  private Daemon server;
  private static final Logger LOG = LoggerFactory.getLogger(DataTransferService.class);

  public void init() throws Exception {
    int bindPort = conf.getInt(SmartConfKeys.SMART_AGENT_DATA_SERVER_PORT_KEY,
        SmartConfKeys.SMART_AGENT_DATA_SERVER_PORT_DEFAULT);
    TcpPeerServer tcpPeerServer = new TcpPeerServer(3000,
        new InetSocketAddress(bindPort), 128);
    threadGroup = new ThreadGroup("DataTransferHandler");
    threadGroup.setDaemon(true);
    xServer = new DataXceiverServer(conf, tcpPeerServer, threadGroup);
    server = new Daemon(threadGroup, xServer);
  }

  public void start() {
    LOG.info("Starting DataTransferService ...");
    if (server != null) {
      server.start();
    }
    LOG.info("DataTransferService started!");
  }

  public void stop() {
    LOG.info("Stopping DataTransferService ...");
    if (server != null) {
      try {
        server.interrupt();
      } catch (Exception e) {
        LOG.warn("Exception when stop", e);
      }
    }

    if (threadGroup != null) {
      int nRetry = 10;
      while (threadGroup.activeCount() != 0 && nRetry > 0) {
        nRetry--;
        threadGroup.interrupt();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }
    }

    if (server != null) {
      try {
        server.join();
      } catch (InterruptedException e) {
      }
    }
    LOG.info("DataTransferService stopped!");
  }
}
