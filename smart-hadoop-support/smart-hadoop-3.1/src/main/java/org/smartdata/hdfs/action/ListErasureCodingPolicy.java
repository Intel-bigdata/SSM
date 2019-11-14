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

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.HadoopUtil;

import java.util.Map;

/**
 * An action to list the info for all EC policies in HDFS.
 */
@ActionSignature(
    actionId = "listec",
    displayName = "listec",
    usage = "No args"
)
public class ListErasureCodingPolicy extends HdfsAction {
  private SmartConf conf;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
  }

  @Override
  public void execute() throws Exception {
    this.setDfsClient(HadoopUtil.getDFSClient(
        HadoopUtil.getNameNodeUri(conf), conf));
    for (ErasureCodingPolicyInfo policyInfo : dfsClient.getErasureCodingPolicies()) {
      appendLog("{" + policyInfo.toString() + "}");
    }
  }
}
