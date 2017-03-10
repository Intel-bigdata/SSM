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
package org.apache.hadoop.ssm.protocol;

import org.apache.hadoop.ssm.RuleInfo;
import org.apache.hadoop.ssm.RuleState;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

/**
 * SSM client can communicate with SSM through this protocol.
 */
public interface ClientProtocol {

  /**
   * Submit a rule to SSM.
   * @param rule rule string
   * @return unique id for the rule
   * @throws IOException
   */
  long submitRule(String rule, RuleState initState) throws IOException;

  /**
   * List rules in the specified states in SSM.
   * @param rulesInStates
   * @return
   * @throws IOException
   */
  List<RuleInfo> listRules(EnumSet<RuleState> rulesInStates) throws IOException;

  /**
   * Execute command through SSM.
   * TODO: Command check to avoid security issues.
   *
   * @param command
   * @return unique id of the command,
   *           can be used to query info about this command.
   * @throws IOException
   */
  long executeCommand(String command) throws IOException;
}
