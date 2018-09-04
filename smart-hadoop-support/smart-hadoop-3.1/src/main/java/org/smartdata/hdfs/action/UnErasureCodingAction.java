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

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.smartdata.action.ActionException;
import org.smartdata.conf.SmartConf;

import java.util.Map;

public class UnErasureCodingAction extends ErasureCodingBase {
  private static final String ecPolicyName = ErasureCodeConstants.REPLICATION_POLICY_NAME;
  private SmartConf conf;

  @Override
  public void init(Map<String, String> args) {
    this.conf = getContext().getConf();
    this.srcPath = args.get(FILE_PATH);
    if (args.containsKey(DEST)) {
      // dest path is under /system/ssm/ec_tmp/file_name_timestamp, the file name is srcName_millisecond
      this.destPath = args.get(DEST);
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