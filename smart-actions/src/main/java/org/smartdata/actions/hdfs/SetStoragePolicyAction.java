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

import java.util.Map;

/**
 * Set storage policy
 */
public class SetStoragePolicyAction extends HdfsAction {
  private static final Logger LOG = LoggerFactory.getLogger(
      SetStoragePolicyAction.class);
  public static final String STORAGE_POLICY = "-storagePolicy";

  private String fileName;
  private String storagePolicy;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.fileName = args.get(FILE_PATH);
    this.storagePolicy = args.get(STORAGE_POLICY);
  }

  @Override
  protected void execute() throws Exception {
    dfsClient.setStoragePolicy(fileName, storagePolicy);
  }
}
