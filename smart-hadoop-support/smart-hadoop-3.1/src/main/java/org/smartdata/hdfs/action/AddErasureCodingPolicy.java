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

import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.smartdata.action.ActionException;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.utils.StringUtil;

import java.util.Map;

/**
 * An action to add an EC policy.
 */
@ActionSignature(
    actionId = "addec",
    displayName = "addec",
    usage = AddErasureCodingPolicy.CODEC_NAME + " $codeName" +
        AddErasureCodingPolicy.DATA_UNITS_NUM + " $dataNum" +
        AddErasureCodingPolicy.PARITY_UNITS_NUM + " $parityNum" +
        AddErasureCodingPolicy.CELL_SIZE + " $cellSize"
)
public class AddErasureCodingPolicy extends HdfsAction {
  public static final String CODEC_NAME = "-codec";
  public static final String DATA_UNITS_NUM = "-dataNum";
  public static final String PARITY_UNITS_NUM = "-parityNum";
  public static final String CELL_SIZE = "-cellSize";
  private SmartConf conf;
  private String policyName;
  private String codecName;
  private int numDataUnits;
  private int numParityUnits;
  private int cellSize;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.conf = getContext().getConf();
    this.codecName = args.get(CODEC_NAME);
    this.numDataUnits = Integer.parseInt(args.get(DATA_UNITS_NUM));
    this.numParityUnits = Integer.parseInt(args.get(PARITY_UNITS_NUM));
    this.cellSize = (int) StringUtil.parseToByte(args.get(CELL_SIZE));
  }

  @Override
  public void execute() throws Exception {
    this.setDfsClient(HadoopUtil.getDFSClient(
        HadoopUtil.getNameNodeUri(conf), conf));
    if (codecName == null || codecName.isEmpty() ||
        numDataUnits <= 0 || numParityUnits <= 0 || cellSize <= 0) {
      throw new ActionException("Illegal EC policy Schema!");
    }
    ECSchema ecSchema = new ECSchema(codecName, numDataUnits, numParityUnits);
    ErasureCodingPolicy ecPolicy = new ErasureCodingPolicy(ecSchema, cellSize);
    AddErasureCodingPolicyResponse addEcResponse =
        dfsClient.addErasureCodingPolicies(new ErasureCodingPolicy[]{ecPolicy})[0];
    if (addEcResponse.isSucceed()) {
      appendResult(String.format("EC policy named %s is added successfully!",
          addEcResponse.getPolicy().getName()));
    } else {
      appendResult(String.format("Failed to add the given EC policy!"));
      throw new ActionException(addEcResponse.getErrorMsg());
    }
  }
}
