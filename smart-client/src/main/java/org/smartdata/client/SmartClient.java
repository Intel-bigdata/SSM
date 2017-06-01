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
package org.smartdata.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.smartdata.client.protocolPB.SmartClientProtocolClientSideTranslatorPB;
import org.smartdata.common.protocol.SmartClientProtocol;
import org.smartdata.common.protocolPB.SmartClientProtocolPB;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metrics.FileAccessEvent;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SmartClient implements java.io.Closeable, SmartClientProtocol {
  private final static long VERSION = 1;
  private Configuration conf;
  private SmartClientProtocol server;
  private volatile boolean running = true;

  public SmartClient(Configuration conf) throws IOException {
    this.conf = conf;
    String rpcConfValue = conf.get(SmartConfKeys.DFS_SSM_RPC_ADDRESS_KEY);
    if (rpcConfValue == null) {
      throw new IOException("SmartServer address not found. Please configure "
          + "it through " + SmartConfKeys.DFS_SSM_RPC_ADDRESS_KEY);
    }

    String[] strings = rpcConfValue.split(":");
    InetSocketAddress address = new InetSocketAddress(
        strings[strings.length - 2],
        Integer.parseInt(strings[strings.length - 1]));
    initialize(address);
  }

  public SmartClient(InetSocketAddress address) throws IOException {
    initialize(address);
  }

  private void initialize(InetSocketAddress address) throws IOException {
    RPC.setProtocolEngine(conf, SmartClientProtocolPB.class,
        ProtobufRpcEngine.class);
    SmartClientProtocolPB proxy = RPC.getProxy(
        SmartClientProtocolPB.class, VERSION, address, conf);
    this.server = new SmartClientProtocolClientSideTranslatorPB(proxy);
  }

  @Override
  public void reportFileAccessEvent(FileAccessEvent event)
      throws IOException {
    checkOpen();
    server.reportFileAccessEvent(event);
  }


  public void checkOpen() throws IOException {
    if (!running) {
      throw new IOException("SmartClient closed");
    }
  }

  @Override
  public void close() {
    if (running) {
      running = false;
      RPC.stopProxy(server);
      server = null;
    }
  }
}
