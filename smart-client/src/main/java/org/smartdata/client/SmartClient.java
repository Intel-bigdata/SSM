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
import org.smartdata.SmartConstants;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.FileState;
import org.smartdata.model.NormalFileState;
import org.smartdata.protocol.SmartClientProtocol;
import org.smartdata.protocol.protobuffer.ClientProtocolClientSideTranslator;
import org.smartdata.protocol.protobuffer.ClientProtocolProtoBuffer;
import org.smartdata.utils.StringUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class SmartClient implements java.io.Closeable, SmartClientProtocol {
  private static final long VERSION = 1;
  private Configuration conf;
  /** The server queue keeps server's order according to active status. **/
  private Deque<SmartClientProtocol> serverQue;
  /** The map from server to its rpc address in "hostname:port" format. **/
  private Map<SmartClientProtocol, String> serverToRpcAddr;
  private volatile boolean running = true;
  private List<String> ignoreAccessEventDirs;
  private Map<String, Integer> singleIgnoreList;
  private List<String> coverAccessEventDirs;
  public static final String ACTIVE_SMART_SERVER_FILE_PATH = "/tmp/active_smart_server";

  public SmartClient(Configuration conf) throws IOException {
    this.conf = conf;
    this.serverQue = new LinkedList<>();
    this.serverToRpcAddr = new HashMap<>();
    this.ignoreAccessEventDirs = new ArrayList<>();
    this.coverAccessEventDirs = new ArrayList<>();
    this.singleIgnoreList = new ConcurrentHashMap<>(200);

    String[] rpcConfValue =
        conf.getTrimmedStrings(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    if (rpcConfValue == null || rpcConfValue.length == 0) {
      throw new IOException("SmartServer address not found. Please configure "
          + "it through " + SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY);
    }
    List<InetSocketAddress> addrList = new LinkedList<>();
    for (String rpcValue : rpcConfValue) {
      String[] hostAndPort = rpcValue.split(":");
      try {
        InetSocketAddress smartServerAddress = new InetSocketAddress(
            hostAndPort[hostAndPort.length - 2],
            Integer.parseInt(hostAndPort[hostAndPort.length - 1]));
        addrList.add(smartServerAddress);
      } catch (Exception e) {
        throw new IOException("Incorrect SmartServer address. Please follow "
            + "IP/Hostname:Port format");
      }
    }
    initialize(addrList.toArray(new InetSocketAddress[addrList.size()]));
  }

  public SmartClient(Configuration conf, InetSocketAddress address)
      throws IOException {
    this.conf = conf;
    this.serverQue = new LinkedList<>();
    this.serverToRpcAddr = new HashMap<>();
    this.ignoreAccessEventDirs = new ArrayList<>();
    this.coverAccessEventDirs = new ArrayList<>();
    this.singleIgnoreList = new ConcurrentHashMap<>(200);

    initialize(new InetSocketAddress[]{address});
  }

  public SmartClient(Configuration conf, InetSocketAddress[] addrs)
      throws IOException {
    this.conf = conf;
    this.serverQue = new LinkedList<>();
    this.serverToRpcAddr = new HashMap<>();
    this.ignoreAccessEventDirs = new ArrayList<>();
    this.coverAccessEventDirs = new ArrayList<>();
    this.singleIgnoreList = new ConcurrentHashMap<>(200);

    initialize(addrs);
  }

  private void initialize(InetSocketAddress[] addrs) throws IOException {
    RPC.setProtocolEngine(conf, ClientProtocolProtoBuffer.class,
        ProtobufRpcEngine.class);
    List<InetSocketAddress> orderedAddrs = new ArrayList<>();
    InetSocketAddress recordedActiveAddr = getActiveServerAddress();
    if (recordedActiveAddr != null) {
      orderedAddrs.add(recordedActiveAddr);
    }
    for (InetSocketAddress addr : addrs) {
      if (!addr.equals(recordedActiveAddr)) {
        orderedAddrs.add(addr);
      }
    }
    for (InetSocketAddress addr : orderedAddrs) {
      ClientProtocolProtoBuffer proxy = RPC.getProxy(
          ClientProtocolProtoBuffer.class, VERSION, addr, conf);
      SmartClientProtocol server = new ClientProtocolClientSideTranslator(proxy);
      serverQue.addLast(server);
      serverToRpcAddr.put(server, addr.getHostName() + ":" + addr.getPort());
    }

    // SMART_IGNORE_DIRS_KEY and SMART_WORK_DIR_KEY should be configured on
    // application side if its dfsClient is replaced by SmartDfsClient.
    Collection<String> ignoreDirs = conf.getTrimmedStringCollection(
        SmartConfKeys.SMART_IGNORE_DIRS_KEY);
    // The system folder and SSM work folder should be ignored to
    // report access count.
    ignoreDirs.add(SmartConstants.SYSTEM_FOLDER);
    ignoreDirs.add(conf.get(SmartConfKeys.SMART_WORK_DIR_KEY,
        SmartConfKeys.SMART_WORK_DIR_DEFAULT));
    for (String s : ignoreDirs) {
      ignoreAccessEventDirs.add(s + (s.endsWith("/") ? "" : "/"));
    }

    Collection<String> coverDirs = conf.getTrimmedStringCollection(
        SmartConfKeys.SMART_COVER_DIRS_KEY);
    for (String s : coverDirs) {
      coverAccessEventDirs.add(s + (s.endsWith("/") ? "" : "/"));
    }
  }

  private void checkOpen() throws IOException {
    if (!running) {
      throw new IOException("SmartClient closed");
    }
  }

  /**
   * Record active server (hostname:port) currently found into a local file.
   * This file can be dropped by OS, but considering it's just used for
   * optimization, the lack of recorded active server doesn't cause critical
   * issue.
   */
  private void recordActiveServerAddr(String addr) {
    FileWriter fw = null;
    try {
      if (!new File(ACTIVE_SMART_SERVER_FILE_PATH).exists()) {
        new File(ACTIVE_SMART_SERVER_FILE_PATH).createNewFile();
      }
      fw = new FileWriter(ACTIVE_SMART_SERVER_FILE_PATH);
      fw.write(addr);
    } catch (IOException e) {
      // Nothing to do.
    } finally {
      if (fw != null) {
        try {
          fw.close();
        } catch (IOException e) {
          // Nothing to do.
        }
      }
    }
  }

  /**
   * Get recorded active server address (hostname:port).
   *
   * @return active server address if found. Otherwise, null.
   */
  private InetSocketAddress getActiveServerAddress() {
    try {
      Scanner scanner = new Scanner(new File(ACTIVE_SMART_SERVER_FILE_PATH));
      if (scanner.hasNextLine()) {
        String address = scanner.nextLine();
        String[] strings = address.split(":");
        return new InetSocketAddress(strings[0], Integer.valueOf(strings[1]));
      }
    } catch (FileNotFoundException e) {
      return null;
    }
    return null;
  }

  /**
   * Reports access count event to smart server. In SSM HA mode, multiple
   * smart servers can be configured. If fail to connect to one server,
   * this method will pick up the next one from a queue to try again. If
   * all servers cannot be connected, an exception will be thrown.
   * <p></p>
   * Generally, Configuration class has only one instance. If this method
   * finds active server has been changed, it will reset the value for
   * property SMART_SERVER_RPC_ADDRESS_KEY in Configuration instance. Thus,
   * next time a SmartClient is created with this Configuration instance,
   * active server will be put in the head of a queue and it will be picked
   * up firstly.
   *
   * @param event
   * @throws IOException
   */
  @Override
  public void reportFileAccessEvent(FileAccessEvent event)
      throws IOException {
    if (!shouldIgnore(event.getPath())) {
      checkOpen();
      int failedServerNum = 0;
      while (true) {
        try {
          SmartClientProtocol server = serverQue.getFirst();
          server.reportFileAccessEvent(event);
          if (failedServerNum != 0) {
            onNewActiveSmartServer();
          }
          break;
        } catch (ConnectException e) {
          failedServerNum++;
          // If all servers has been tried but still fail,
          // throw an exception.
          if (failedServerNum == serverQue.size()) {
            throw new ConnectException("Tried to connect to configured SSM "
                + "server(s), but failed." + e.getMessage());
          }
          // Move the first server to last.
          serverQue.addLast(serverQue.pollFirst());
        }
      }
    }
  }

  /**
   * Reset smart server address in conf and a local file to reflect the
   * changes of active smart server in fail over.
   */
  public void onNewActiveSmartServer() {
    List<String> rpcAddrs = new LinkedList<>();
    for (SmartClientProtocol s : serverQue) {
      rpcAddrs.add(serverToRpcAddr.get(s));
    }
    conf.set(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY,
        StringUtil.join(",", rpcAddrs));
    String addr = serverToRpcAddr.get(serverQue.getFirst());
    recordActiveServerAddr(addr);
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
