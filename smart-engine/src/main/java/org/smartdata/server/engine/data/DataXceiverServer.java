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
import org.smartdata.server.engine.data.net.NetUtil;
import org.smartdata.server.engine.data.net.Peer;
import org.smartdata.server.engine.data.net.PeerServer;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

public class DataXceiverServer implements Runnable {
  private final PeerServer peerServer;
  private int maxNumHandler;
  private SmartConf conf;
  ThreadGroup threadGroup = null;

  private final Map<Peer, Thread> peers = new HashMap<>();
  private final Map<Peer, DataXceiver> peersXceiver = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(DataXceiverServer.class);

  public DataXceiverServer(SmartConf conf, PeerServer peerServer, ThreadGroup threadGroup) {
    this.conf = conf;
    this.peerServer = peerServer;
    this.threadGroup = threadGroup;
    maxNumHandler = conf.getInt(SmartConfKeys.SMART_AGENT_MAX_DATA_HANDLER_COUNT_KEY,
        SmartConfKeys.SMART_AGNET_MAX_DATA_HANDLER_COUNT_DEFAULT);
  }

  public void initServer() {
  }

  @Override
  public void run() {
    Peer peer = null;
    boolean closed = false;
    while (!closed) {
      try {
        peer = peerServer.accept();
        int activeCount = threadGroup.activeCount();
        if (activeCount > maxNumHandler) {
          throw new IOException("Active handlers exceeds the maximum limit " + maxNumHandler);
        }

        new Daemon(threadGroup, new DataXceiver()).start();
      } catch (SocketTimeoutException e) {
      } catch (IOException e) {
        NetUtil.cleanup(LOG, peer);
      } catch (OutOfMemoryError e) {
        NetUtil.cleanup(LOG, peer);
      } catch (Throwable t) {
        LOG.error("Error happened:", t);
        closed = true;
      }
    }

    try {
      peerServer.close();
    } catch (IOException e) {
      LOG.warn("Exception while closing ", e);
    }
  }
}
