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
package org.smartdata.hdfs.action;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.conf.SmartConf;

import java.util.HashMap;
import java.util.Map;

public class ErasureCodingAction extends ErasureCodingBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ErasureCodingAction.class);
  public static final String EC_POLICY_NAME = "-policy";

  private String ecPolicyName;
  private SmartConf conf;

  @Override
  public void init(Map<String, String> args) {
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(DEST)) {
      // It's a temp file kept for converting a file to another with other ec policy
      this.destPath = args.get(DEST);
    }
    if (args.containsKey(EC_POLICY_NAME)) {
      this.ecPolicyName = args.get(EC_POLICY_NAME);
    } else {
      this.conf = getContext().getConf();
      String defaultEcPolicy = conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY,
          DFSConfigKeys.DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT);
      this.ecPolicyName = defaultEcPolicy;
    }
    if (args.containsKey(BUF_SIZE)) {
      this.bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
    this.progress = 0.0F;
  }

  @Override
  protected void execute() throws Exception {
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(srcPath);
    if (fileStatus == null) {
      throw new ActionException("File doesn't exist!");
    }
    ValidateEcPolicy(ecPolicyName);
    if (fileStatus.getErasureCodingPolicy().getName().equals(ecPolicyName)) {
      this.progress = 1.0F;
      return;
    }
    if (fileStatus.isDir()) {
      dfsClient.setErasureCodingPolicy(srcPath, ecPolicyName);
      progress = 1.0F;
      return;
    }
    convert(conf, ecPolicyName);
    dfsClient.rename(destPath, srcPath, null);
  }

  @Override
  public float getProgress() {
    return progress;
  }
}