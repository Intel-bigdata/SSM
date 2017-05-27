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
package org.smartdata.server.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.smartdata.common.actions.ActionType;
import org.smartdata.server.actions.mover.MoverPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

/**
 * MoveFile Action
 */
public class MoveFile implements Action {
  private static final Logger LOG = LoggerFactory.getLogger(MoveFile.class);

  public String storagePolicy;
  private String fileName;
  private Configuration conf;
  private ActionType actionType;
  private DFSClient dfsClient;
  private String name = "MoveFile";


  public MoveFile() {
    this.actionType = ActionType.MoveFile;
  }

  public String getName() {
    return name;
  }

  public Action initial(DFSClient client, Configuration conf, String[] args) {
    this.dfsClient = client;
    this.conf = conf;
    this.fileName = args[0];
    this.storagePolicy = args[1];
    return this;
  }

  /**
   * Execute an action.
   *
   * @return true if success, otherwise return false.
   */
  public UUID run() {
    return runMove(fileName);
  }

  private UUID runMove(String fileName) {
    // TODO check if storagePolicy is the same
    LOG.info("Action starts at {} : {} -> {}",
        new Date(System.currentTimeMillis()), fileName, storagePolicy);
    try {
      dfsClient.setStoragePolicy(fileName, storagePolicy);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return MoverPool.getInstance().createMoverAction(fileName);
  }
}