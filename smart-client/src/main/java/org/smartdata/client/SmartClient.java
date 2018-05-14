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
import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileState;
import org.smartdata.protocol.SmartClientProtocol;
import org.smartdata.protocol.protobuffer.ClientProtocolClientSideTranslator;
import org.smartdata.protocol.protobuffer.ClientProtocolProtoBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class SmartClient implements java.io.Closeable, SmartClientProtocol {
  private static final long VERSION = 1;
  private static final int CACHE_SIZE = 1000;
  private Configuration conf;
  private SmartClientProtocol server;
  private volatile boolean running = true;
  private List<String> ignoreAccessEventDirs;
  private Map<String, FileState> fileStateCache;
  private SmallFileBloomFilter smallFileBloomFilter;

  private class SmallFileBloomFilter {
    private static final int BIT_SIZE = 2 << 28;
    private final int[] seeds = new int[] { 3, 5, 7, 11, 13, 31, 37, 61 };
    private BitSet bitSet = new BitSet(BIT_SIZE);
    private BloomHash[] bloomHashes = new BloomHash[seeds.length];
    private List<String> whiteList = new ArrayList<>();

    private SmallFileBloomFilter() {
      for (int i = 0; i < seeds.length; i++) {
        bloomHashes[i] = new BloomHash(BIT_SIZE, seeds[i]);
      }
    }

    private void add(String element) {
      if (element == null) {
        return;
      }

      // Add element to bit set
      for (BloomHash bloomHash : bloomHashes) {
        bitSet.set(bloomHash.hash(element));
      }
    }

    private boolean contains(String value) {
      if (value == null) {
        return false;
      }

      for (BloomHash bloomHash : bloomHashes) {
        if (!bitSet.get(bloomHash.hash(value))) {
          return false;
        }
      }

      return true;
    }

    private class BloomHash {
      private int size;
      private int seed;

      private BloomHash(int cap, int seed) {
        this.size = cap;
        this.seed = seed;
      }

      /**
       * Calculate the index of value in bit set through hash.
       */
      private int hash(String value) {
        int result = 0;
        int len = value.length();
        for (int i = 0; i < len; i++) {
          result = seed * result + value.charAt(i);
        }

        return (size - 1) & result;
      }
    }
  }

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
    initialize(address);
  }

  public SmartClient(Configuration conf, InetSocketAddress address)
      throws IOException {
    this.conf = conf;
    initialize(address);
  }

  private void initialize(InetSocketAddress address) throws IOException {
    RPC.setProtocolEngine(conf, ClientProtocolProtoBuffer.class,
        ProtobufRpcEngine.class);
    ClientProtocolProtoBuffer proxy = RPC.getProxy(
        ClientProtocolProtoBuffer.class, VERSION, address, conf);
    server = new ClientProtocolClientSideTranslator(proxy);
    Collection<String> dirs = conf.getTrimmedStringCollection(
        SmartConfKeys.SMART_IGNORE_DIRS_KEY);
    ignoreAccessEventDirs = new ArrayList<>();
    for (String s : dirs) {
      ignoreAccessEventDirs.add(s + (s.endsWith("/") ? "" : "/"));
    }
    fileStateCache = new LinkedHashMap<String, FileState>(
        CACHE_SIZE, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(
          Map.Entry<String, FileState> eldest) {
        return size() > CACHE_SIZE;
      }
    };
    smallFileBloomFilter = new SmallFileBloomFilter();
    for (String smallFile : getSmallFileList()) {
      smallFileBloomFilter.add(smallFile);
    }
  }

  private void checkOpen() throws IOException {
    if (!running) {
      throw new IOException("SmartClient closed");
    }
  }

  private boolean shouldIgnore(String path) {
    String toCheck = path.endsWith("/") ? path : path + "/";
    for (String s : ignoreAccessEventDirs) {
      if (toCheck.startsWith(s)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if the specified file is small file.
   *
   * @param filePath the specified small file
   * @param useBloomFilter whether use bloom filter to check
   * @throws IOException if exception occur
   */
  public boolean isSmallFile(String filePath, boolean useBloomFilter)
      throws IOException {
    if (useBloomFilter) {
      return !smallFileBloomFilter.whiteList.contains(filePath)
          && smallFileBloomFilter.contains(filePath);
    } else {
      return (getFileState(filePath) instanceof CompactFileState);
    }
  }

  /**
   * Add element to small file bloom filter.
   *
   * @param filePath the specified small file
   */
  public void addElementToBF(String filePath) {
    smallFileBloomFilter.add(filePath);
  }

  /**
   * Add excluded element to small file bloom filter.
   *
   * @param filePath the specified small file
   */
  public void addExcludedElementToBF(String filePath) {
    smallFileBloomFilter.whiteList.add(filePath);
  }

  @Override
  public List<String> getSmallFileList() throws IOException {
    checkOpen();
    return server.getSmallFileList();
  }

  @Override
  public void reportFileAccessEvent(FileAccessEvent event)
      throws IOException {
    if (!shouldIgnore(event.getPath())) {
      checkOpen();
      server.reportFileAccessEvent(event);
    }
  }

  @Override
  public FileState getFileState(String filePath) throws IOException {
    checkOpen();
    if (!fileStateCache.containsKey(filePath)) {
      FileState fileState = server.getFileState(filePath);
      fileStateCache.put(filePath, fileState);
      return fileState;
    } else {
      return fileStateCache.get(filePath);
    }
  }

  @Override
  public List<FileState> getFileStates(String filePath) throws IOException {
    checkOpen();
    return server.getFileStates(filePath);
  }

  /**
   * Cache compact file states of the small files
   * whose container file is same as the specified small file's.
   *
   * @param filePath the specified small file
   * @throws IOException if exception occur
   */
  public void cacheCompactFileStates(String filePath) throws IOException {
    List<FileState> fileStates = getFileStates(filePath);
    for (FileState filestate : fileStates) {
      String key = filestate.getPath();
      fileStateCache.put(key, filestate);
    }
  }

  @Override
  public void updateFileState(FileState fileState)
      throws IOException {
    checkOpen();
    // Add new element to bloom filter if it's small file
    if (fileState instanceof CompactFileState) {
      addElementToBF(fileState.getPath());
    }
    server.updateFileState(fileState);
  }

  @Override
  public void deleteFileState(String filePath, boolean recursive)
      throws IOException {
    checkOpen();
    server.deleteFileState(filePath, recursive);
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
