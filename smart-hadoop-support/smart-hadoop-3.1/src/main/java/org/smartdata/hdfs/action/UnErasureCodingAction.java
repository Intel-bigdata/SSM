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

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionException;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.HadoopUtil;

import java.util.Map;

@ActionSignature(
    actionId = "unec",
    displayName = "unec",
    usage = HdfsAction.FILE_PATH + " $src " + ErasureCodingBase.BUF_SIZE + " $bufSize"
)
public class UnErasureCodingAction extends ErasureCodingBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(UnErasureCodingAction.class);
  private static final String ecPolicyName = ErasureCodeConstants.REPLICATION_POLICY_NAME;
  private SmartConf conf;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(EC_TMP)) {
      this.ecTmpPath = args.get(EC_TMP);
    }
    if (args.containsKey(ORIGIN_TMP)) {
      this.originTmpPath = args.get(ORIGIN_TMP);
    }
    if (args.containsKey(BUF_SIZE)) {
      this.bufferSize = Integer.valueOf(args.get(BUF_SIZE));
    }
    this.progress = 0.0F;
  }

  @Override
  protected void execute() throws Exception {
    // keep attribute consistent
    //
    this.setDfsClient(HadoopUtil.getDFSClient(
        HadoopUtil.getNameNodeUri(conf), conf));
    HdfsFileStatus fileStatus = dfsClient.getFileInfo(srcPath);
    if (fileStatus == null) {
      throw new ActionException("File doesn't exist!");
    }
    ValidateEcPolicy(ecPolicyName);
    ErasureCodingPolicy srcEcPolicy = fileStatus.getErasureCodingPolicy();
    if (srcEcPolicy != null) {
      if (srcEcPolicy.getName().equals(ecPolicyName)) {
        this.progress = 1.0F;
        return;
      }
    }
    if (fileStatus.isDir()) {
      dfsClient.setErasureCodingPolicy(srcPath, ecPolicyName);
      progress = 1.0F;
      return;
    }
    convert(conf, ecPolicyName);
    dfsClient.rename(srcPath, originTmpPath, null);
    dfsClient.rename(ecTmpPath, srcPath, null);
    if (!isEquivalence(originTmpPath, srcPath)) {
      dfsClient.delete(srcPath, false);
      dfsClient.rename(originTmpPath, srcPath, null);
      LOG.warn("The original file is modified during the conversion.");
      throw new ActionException("The original file is modified during the conversion.");
    } else {
      dfsClient.delete(originTmpPath, false);
    }
  }

  @Override
  public float getProgress() {
    return progress;
  }
}