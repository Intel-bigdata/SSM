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

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.HadoopUtil;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

@ActionSignature(
    actionId = "ec",
    displayName = "ec",
    usage = HdfsAction.FILE_PATH + " $src " + ErasureCodingAction.EC_POLICY_NAME + " $policy" +
        ErasureCodingBase.BUF_SIZE + " $bufSize"
)
public class ErasureCodingAction extends ErasureCodingBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ErasureCodingAction.class);
  public static final String EC_POLICY_NAME = "-policy";

  private SmartConf conf;
  private String ecPolicyName;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(EC_TMP)) {
      // this is a temp file kept for converting a file to another with other ec policy.
      this.ecTmpPath = args.get(EC_TMP);
    }
    if (args.containsKey(EC_POLICY_NAME)) {
      this.ecPolicyName = args.get(EC_POLICY_NAME);
    } else {
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
    this.setDfsClient(HadoopUtil.getDFSClient(
        HadoopUtil.getNameNodeUri(conf), conf));
    // keep attribute consistent
    //
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(srcPath);
    if (fileStatus == null) {
      throw new ActionException("File doesn't exist!");
    }
    ValidateEcPolicy(ecPolicyName);
    ErasureCodingPolicy srcEcPolicy = fileStatus.getErasureCodingPolicy();
    // if the current ecPolicy is already the target one, no need to convert
    if (srcEcPolicy != null){
      if (srcEcPolicy.getName().equals(ecPolicyName)) {
        this.progress = 1.0F;
        return;
      }
    }
    if (fileStatus.isDir()) {
      dfsClient.setErasureCodingPolicy(srcPath, ecPolicyName);
      this.progress = 1.0F;
      return;
    }
    HdfsDataOutputStream outputStream = null;
    try {
      // append the file to acquire the lock to avoid modifying, no real appending occurs.
      outputStream =
          dfsClient.append(srcPath, bufferSize, EnumSet.of(CreateFlag.APPEND), null, null);
      convert(conf, ecPolicyName);
      dfsClient.rename(ecTmpPath, srcPath, Options.Rename.OVERWRITE);
    } catch (ActionException ex) {
      throw new ActionException(ex);
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
    }
  }

  public void ValidateEcPolicy(String ecPolicyName) throws Exception {
    Map<String, ErasureCodingPolicyState> ecPolicyNameToState = new HashMap<>();
    for (ErasureCodingPolicyInfo info : dfsClient.getErasureCodingPolicies()) {
      ecPolicyNameToState.put(info.getPolicy().getName(), info.getState());
    }
    if (!ecPolicyNameToState.keySet().contains(ecPolicyName)) {
      throw new ActionException("The EC policy " + ecPolicyName + " is not supported!");
    } else if (ecPolicyNameToState.get(ecPolicyName) == ErasureCodingPolicyState.DISABLED
        || ecPolicyNameToState.get(ecPolicyName) == ErasureCodingPolicyState.REMOVED) {
      throw new ActionException("The EC policy " + ecPolicyName + " is not enabled!");
    }
  }

  @Override
  public float getProgress() {
    return progress;
  }
}