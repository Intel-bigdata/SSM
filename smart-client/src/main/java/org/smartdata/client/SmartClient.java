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
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.FileState;
import org.smartdata.model.NormalFileState;
import org.smartdata.protocol.SmartClientProtocol;
import org.smartdata.protocol.protobuffer.ClientProtocolClientSideTranslator;
import org.smartdata.protocol.protobuffer.ClientProtocolProtoBuffer;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SmartClient implements java.io.Closeable, SmartClientProtocol {
  private static final long VERSION = 1;
  private Configuration conf;
  private Deque<SmartClientProtocol> serverQue;
  private volatile boolean running = true;
  private List<String> ignoreAccessEventDirs;
  private Map<String, Integer> singleIgnoreList;
  private List<String> coverAccessEventDirs;

  public SmartClient(Configuration conf) throws IOException {
    this.conf = conf;
    String rpcConfValue = conf.get(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    if (rpcConfValue == null) {
      throw new IOException("SmartServer address not found. Please configure "
          + "it through " + SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    }

    String[] strings = rpcConfValue.split(":");
    InetSocketAddress address;
    try {
      address = new InetSocketAddress(
          strings[strings.length - 2],
          Integer.parseInt(strings[strings.length - 1]));
    } catch (Exception e) {
      throw new IOException("Incorrect SmartServer address. Please follow the "
          + "IP/Hostname:Port format");
    }
    initialize(new InetSocketAddress[]{address});
  }

  public SmartClient(Configuration conf, InetSocketAddress address)
      throws IOException {
    this.conf = conf;
    initialize(new InetSocketAddress[]{address});
  }

  public SmartClient(Configuration conf, InetSocketAddress[] addrs)
      throws IOException {
    this.conf = conf;
    initialize(addrs);
  }

  private void initialize(InetSocketAddress[] addrs) throws IOException {
    this.serverQue = new LinkedList<>();
    RPC.setProtocolEngine(conf, ClientProtocolProtoBuffer.class,
        ProtobufRpcEngine.class);
    for (InetSocketAddress address : addrs) {
      ClientProtocolProtoBuffer proxy = RPC.getProxy(
          ClientProtocolProtoBuffer.class, VERSION, address, conf);
      serverQue.addLast(new ClientProtocolClientSideTranslator(proxy));
    }

    // The below two properties should be configured on HDFS side
    // if its dfsClient is replaced by SmartDfsClient.
    Collection<String> ignoreDirs = conf.getTrimmedStringCollection(
        SmartConfKeys.SMART_IGNORE_DIRS_KEY);
    Collection<String> coverDirs = conf.getTrimmedStringCollection(
        SmartConfKeys.SMART_COVER_DIRS_KEY);
    ignoreAccessEventDirs = new ArrayList<>();
    coverAccessEventDirs = new ArrayList<>();
    for (String s: ignoreDirs) {
      ignoreAccessEventDirs.add(s + (s.endsWith("/") ? "" : "/"));
    }
    for (String s: coverDirs) {
      coverAccessEventDirs.add(s + (s.endsWith("/") ? "" : "/"));
    }

    singleIgnoreList = new ConcurrentHashMap<>(200);
  }

  private void checkOpen() throws IOException {
    if (!running) {
      throw new IOException("SmartClient closed");
    }
  }

  @Override
  public void reportFileAccessEvent(FileAccessEvent event)
      throws IOException {
    if (!shouldIgnore(event.getPath())) {
      checkOpen();
      int triedServerNum = 0;
      while (true) {
        try {
          SmartClientProtocol server = serverQue.getFirst();
          server.reportFileAccessEvent(event);
          break;
        } catch (ConnectException e) {
          triedServerNum++;
          // If all servers has been tried, interrupt and throw the exception.
          if (triedServerNum == serverQue.size()) {
            throw new ConnectException("Tried to connect to configured SSM " +
                "server(s), but failed." + e.getMessage());
          }
          // Put the first server to last, and will pick the second one to try.
          serverQue.addLast(serverQue.pollFirst());
        }
      }
    }
  }

  @Override
  public FileState getFileState(String filePath) throws IOException {
    checkOpen();
    int triedServerNum = 0;
    while (true) {
      try {
        SmartClientProtocol server = serverQue.getFirst();
        return server.getFileState(filePath);
      } catch (ConnectException e) {
        triedServerNum++;
        // If all servers has been tried, interrupt and throw the exception.
        if (triedServerNum == serverQue.size()) {
          // client cannot connect to server
          // don't report access event for this file this time
          singleIgnoreList.put(filePath, 0);
          // Assume the given file is normal, but serious error can occur if
          // the file is compacted or compressed by SSM.
          return new NormalFileState(filePath);
        }
        // Put the first server to last, and will pick the second one to try.
        serverQue.addLast(serverQue.pollFirst());
      }
    }
  }

  public boolean shouldIgnore(String path) {
    if (singleIgnoreList.containsKey(path)) {
      // this report should be ignored
      singleIgnoreList.remove(path);
      return true;
    }
    String toCheck = path.endsWith("/") ? path : path + "/";
    for (String s : ignoreAccessEventDirs) {
      if (toCheck.startsWith(s)) {
        return true;
      }
    }
    if (coverAccessEventDirs.isEmpty()) {
      return false;
    }
    for (String s : coverAccessEventDirs) {
      if (toCheck.startsWith(s)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (running) {
      running = false;
      for (SmartClientProtocol server : serverQue) {
        RPC.stopProxy(server);
      }
      serverQue = null;
    }
  }
}
