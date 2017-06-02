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
package org.smartdata.actions.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionStatus;

/**
 * Set storage policy
 */
public class SetStoragePolicyAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(
      SetStoragePolicyAction.class);

  private String fileName;
  private String storagePolicy;

  public SetStoragePolicyAction() {
    this.setActionStatus(new ActionStatus());
  }

  @Override
  public void init(String[] args) {
    super.init(args);
    this.fileName = args[0];
    this.storagePolicy = args[1];
  }

  @Override
  protected void execute() {
    ActionStatus actionStatus = getActionStatus();
    actionStatus.setStartTime();
    try {
      dfsClient.setStoragePolicy(fileName, storagePolicy);
      actionStatus.setSuccessful(true);
    } catch (Exception e) {
      actionStatus.setSuccessful(false);
      throw new RuntimeException(e);
    } finally {
      actionStatus.setFinished(true);
    }
  }
}
