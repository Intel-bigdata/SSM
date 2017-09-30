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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ShortCircuitWriteServer implements Runnable {
  private final Logger LOG = LoggerFactory.getLogger(ShortCircuitWriteServer.class);

  private SmartConf config;
  private volatile boolean shutdown = false;

  private int blockIndex = 0;

  private ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

  public ShortCircuitWriteServer(SmartConf config) {
    this.config = config;
    assert null != config;
  }

  private void init() {
//    try {
//    } catch (IOException e) {
//      LOG.error("[SCW] Error in short circuit write internal initialization:" + e);
//      shutdown = true;
//    }
  }

  public void shutdownServer() {
    shutdown = true;
  }

  private void startServer(int port) {
    try {
      ServerSocketChannel ssc = ServerSocketChannel.open();
      Selector accSel = Selector.open();
      ServerSocket socket = ssc.socket();
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress(port));
      ssc.configureBlocking(false);
      ssc.register(accSel, SelectionKey.OP_ACCEPT);

      while (ssc.isOpen() && !shutdown) {
        if (accSel.select(1000) > 0) {
          Iterator<SelectionKey> it = accSel.selectedKeys().iterator();
          while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();
            if (key.isAcceptable()) {
              handleAccept(key);
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.error("[SCW] Failed in SCW startServer on Port " + port + " :", e);
    }
  }

  private void handleAccept(SelectionKey key) {
    try {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel client = server.accept();
      if (client != null) {
        cachedThreadPool.execute(new WriteHandler(client, blockIndex++));
      }
    } catch (IOException e) {
      LOG.error("[SCW] Failed in SCW handleAccept:", e);
    }
  }

  @Override
  public void run() {
    init();
    int port = config.getInt(SmartConfKeys.SMART_AGENT_DATA_SERVER_PORT_KEY,
        SmartConfKeys.SMART_AGENT_DATA_SERVER_PORT_DEFAULT);
    startServer(port);
  }

  class WriteHandler implements Runnable {
    public final int BUFFER_SIZE = 1024 * 1024;

    private SocketChannel sc;
    private ByteBuffer bb;

    WriteHandler(SocketChannel sc, int index) {
      this.sc = sc;
      bb = ByteBuffer.allocate(BUFFER_SIZE);
    }

    @Override
    public void run() {
      try {

      } finally {
        try {
          sc.close();
        } catch (IOException e) {
          // Close socket channel
        }
      }
    }
  }
}
