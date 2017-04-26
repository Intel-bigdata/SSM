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
package org.apache.hadoop.ssm;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.ssm.sql.tables.AccessCountTableManager;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class AccessCountFetcher {
  private static final Long DEFAULT_INTERVAL = 5 * 1000L;
  private FetchTask fetchTask;
  private Long fetchInterval;
  private Timer timer;

  public AccessCountFetcher(DFSClient client, AccessCountTableManager manager) {
    this(DEFAULT_INTERVAL, client, manager);
  }

  public AccessCountFetcher(Long fetchInterval, DFSClient client,
      AccessCountTableManager manager) {
    this.timer = new Timer();
    this.fetchInterval = fetchInterval;
    this.fetchTask = new FetchTask(client, manager);
  }

  public void start() {
    Long current = System.currentTimeMillis();
    Long toWait = fetchInterval - (current % fetchInterval);
    timer.schedule(fetchTask, toWait, fetchInterval);
  }

  public void stop() {
    this.timer.cancel();
  }

  private static class FetchTask extends TimerTask {
    private final DFSClient client;
    private final AccessCountTableManager manager;

    public FetchTask(DFSClient client, AccessCountTableManager manager) {
      this.client = client;
      this.manager = manager;
    }

    @Override
    public void run() {
      try {
        FilesAccessInfo fileAccess = client.getFilesAccessInfo();
        this.manager.addAccessCountInfo(fileAccess);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
