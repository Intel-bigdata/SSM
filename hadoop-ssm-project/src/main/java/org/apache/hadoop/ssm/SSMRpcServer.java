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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

/**
 * Implements the rpc calls.
 * TODO: Implement statistics for SSM rpc server
 */
public class SSMRpcServer implements SSMProtocols {
  private SSMServer ssm;
  private Configuration conf;

  private final RPC.Server clientRpcServer;

  public SSMRpcServer(SSMServer ssm, Configuration conf) throws IOException {
    this.ssm = ssm;
    this.conf = conf;

    // TODO: implement ssm ClientProtocol
    clientRpcServer = new RPC.Builder(conf).build();
  }

  @Override
  public long submitRule(String rule, RuleState initState) throws IOException {
    return 0L;
  }

  @Override
  public List<RuleInfo> listRules(EnumSet<RuleState> rulesInStates)
    throws IOException {
    return null;
  }

  @Override
  public long executeCommand(String command) throws IOException {
    return 0L;
  }

  /**
   * Start SSM RPC service
   */
  public void start() {
    // TODO: start clientRpcServer
  }

  /**
   * Stop SSM RPC service
   */
  public void stop() {
  }

  /**
   * Waiting for RPC threads to exit.
   */
  public void join() {
  }
}
