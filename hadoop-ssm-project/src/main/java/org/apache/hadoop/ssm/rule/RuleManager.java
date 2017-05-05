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
package org.apache.hadoop.ssm.rule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ssm.SSMServer;
import org.apache.hadoop.ssm.rule.parser.RuleStringParser;
import org.apache.hadoop.ssm.rule.parser.TranslateResult;
import org.apache.hadoop.ssm.sql.CommandInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;

import java.io.IOException;
import java.util.List;

/**
 * Manage and execute rules.
 * We can have 'cache' here to decrease the needs to execute a SQL query.
 */
public class RuleManager {
  private SSMServer ssm;
  private Configuration conf;
  private DBAdapter dbAdapter;

  public RuleManager(SSMServer ssm, Configuration conf, DBAdapter dbAdapter) {
    this.ssm = ssm;
    this.conf = conf;
    this.dbAdapter = dbAdapter;
  }

  /**
   * Submit a rule to RuleManger.
   * @param rule
   * @param initState
   * @return
   * @throws IOException
   */
  public long submitRule(String rule, RuleState initState)
      throws IOException {
    TranslateResult tr = doCheckRule(rule);
    RuleInfo.Builder builder = RuleInfo.newBuilder();
    builder.setRuleText(rule).setState(initState);
    RuleInfo ruleInfo = builder.build();
    if (!dbAdapter.insertNewRule(ruleInfo)) {
      throw new IOException("Create rule failed");
    }
    return ruleInfo.getId();
  }

  private TranslateResult doCheckRule(String rule) throws IOException {
    RuleStringParser parser = new RuleStringParser(rule);
    return parser.translate();
  }

  public void checkRule(String rule) throws IOException {
    doCheckRule(rule);
  }

  /**
   * Delete a rule in SSM. if dropPendingCommands equals false then the rule
   * record will still be kept in Table 'rules', the record will be deleted
   * sometime later.
   *
   * @param ruleID
   * @param dropPendingCommands pending commands triggered by the rule will be
   *                            discarded if true.
   * @throws IOException
   */
  public void DeleteRule(long ruleID, boolean dropPendingCommands)
      throws IOException {
  }

  public void setRuleState(long ruleID, RuleState newState,
      boolean dropPendingCommands) throws IOException {
  }

  public RuleInfo getRuleInfo(long ruleID) throws IOException {
    return dbAdapter.getRuleInfo(ruleID);
  }

  public List<RuleInfo> getAllRuleInfo() throws IOException {
    return dbAdapter.getRuleInfo();
  }

  public void updateRuleInfo(long ruleId, RuleState rs, long lastCheckTime,
      long checkedCount, int commandsGen) {
    dbAdapter.updateRuleInfo(ruleId, rs, lastCheckTime,
        checkedCount, commandsGen);
  }

  //
  public void addNewCommands(List<CommandInfo> commands) {
    dbAdapter.insertCommandsTable((CommandInfo[])commands.toArray());
  }


  /**
   * Init RuleManager, this includes:
   *    1. Load related data from local storage or HDFS
   *    2. Initial
   * @throws IOException
   */
  public void init() throws IOException {
  }

  /**
   * Start services
   */
  public void start() {
  }

  /**
   * Stop services
   */
  public void stop() {
  }

  /**
   * Waiting for threads to exit.
   */
  public void join() {
  }
}
