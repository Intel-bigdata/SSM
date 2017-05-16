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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ssm.ModuleSequenceProto;
import org.apache.hadoop.ssm.SSMServer;
import org.apache.hadoop.ssm.StatesManager;
import org.apache.hadoop.ssm.rule.parser.RuleStringParser;
import org.apache.hadoop.ssm.rule.parser.TranslateResult;
import org.apache.hadoop.ssm.rule.parser.TranslationContext;
import org.apache.hadoop.ssm.sql.CommandInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage and execute rules.
 * We can have 'cache' here to decrease the needs to execute a SQL query.
 */
public class RuleManager implements ModuleSequenceProto {
  private SSMServer ssm;
  private Configuration conf;
  private DBAdapter dbAdapter;
  private boolean isClosed = false;

  private ConcurrentHashMap<Long, RuleContainer> mapRules =
      new ConcurrentHashMap<>();

  // TODO: configurable
  public ExecutorScheduler execScheduler = new ExecutorScheduler(4);

  @VisibleForTesting
  public RuleManager(SSMServer ssm, Configuration conf, DBAdapter dbAdapter) {
    this.ssm = ssm;
    this.conf = conf;
    this.dbAdapter = dbAdapter;
  }

  public RuleManager(SSMServer ssm, Configuration conf) {
    this.ssm = ssm;
    this.conf = conf;
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
    if (initState != RuleState.ACTIVE && initState != RuleState.DISABLED
        && initState != RuleState.DRYRUN) {
      throw new IOException("Invalid initState = " + initState
          + ", it MUST be one of [" + RuleState.ACTIVE
          + ", " + RuleState.DRYRUN + ", " + RuleState.DISABLED + "]");
    }

    TranslateResult tr = doCheckRule(rule, null);
    RuleInfo.Builder builder = RuleInfo.newBuilder();
    builder.setRuleText(rule).setState(initState);
    RuleInfo ruleInfo = builder.build();
    if (!dbAdapter.insertNewRule(ruleInfo)) {
      throw new IOException("Create rule failed");
    }

    RuleContainer container = new RuleContainer(ruleInfo, dbAdapter);
    mapRules.put(ruleInfo.getId(), container);

    submitRuleToScheduler(container.launchExecutor(this));

    return ruleInfo.getId();
  }

  public TranslateResult doCheckRule(String rule, TranslationContext ctx)
      throws IOException {
    RuleStringParser parser = new RuleStringParser(rule, ctx);
    return parser.translate();
  }

  public void checkRule(String rule) throws IOException {
    doCheckRule(rule, null);
  }

  public DBAdapter getDbAdapter() {
    return dbAdapter;
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
    RuleContainer container = checkIfExists(ruleID);
    container.DeleteRule();
  }

  public void ActivateRule(long ruleID) throws IOException {
    RuleContainer container = checkIfExists(ruleID);
    submitRuleToScheduler(container.ActivateRule(this));
  }

  public void DisableRule(long ruleID, boolean dropPendingCommands)
      throws IOException {
    RuleContainer container = checkIfExists(ruleID);
    container.DisableRule();
  }

  private RuleContainer checkIfExists(long ruleID) throws IOException {
    RuleContainer container = mapRules.get(ruleID);
    if (container == null) {
      throw new IOException("Rule with ID = " + ruleID + " not found");
    }
    return container;
  }

  public RuleInfo getRuleInfo(long ruleID) throws IOException {
    RuleContainer container = checkIfExists(ruleID);
    return container.getRuleInfo();
  }

  public List<RuleInfo> listRulesInfo() throws IOException {
    Collection<RuleContainer> containers = mapRules.values();
    List<RuleInfo> retInfos = new ArrayList<>();
    for (RuleContainer container : containers) {
      retInfos.add(container.getRuleInfo());
    }
    return retInfos;
  }

  public void updateRuleInfo(long ruleId, RuleState rs, long lastCheckTime,
      long checkedCount, int commandsGen) throws IOException {
    RuleContainer container = checkIfExists(ruleId);
    container.updateRuleInfo(rs, lastCheckTime, checkedCount, commandsGen);
  }

  public void addNewCommands(List<CommandInfo> commands) {
    if (commands == null || commands.size() == 0) {
      return;
    }

    CommandInfo[] cmds = commands.toArray(new CommandInfo[commands.size()]);
    dbAdapter.insertCommandsTable(cmds);
  }

  public boolean isClosed() {
    return isClosed;
  }

  public StatesManager getStatesManager() {
    return ssm != null ? ssm.getStatesManager() : null;
  }

  /**
   * Init RuleManager, this includes:
   *    1. Load related data from local storage or HDFS
   *    2. Initial
   * @throws IOException
   */
  public boolean init(DBAdapter dbAdapter) throws IOException {
    this.dbAdapter = dbAdapter;
    // Load rules table
    List<RuleInfo> rules = dbAdapter.getRuleInfo();
    for (RuleInfo rule : rules) {
      mapRules.put(rule.getId(), new RuleContainer(rule, dbAdapter));
    }
    return true;
  }

  private void submitRuleToScheduler(RuleQueryExecutor executor)
      throws IOException {
    if (executor == null || executor.isExited()) {
      return;
    }
    execScheduler.addPeriodicityTask(executor);
  }

  /**
   * Start services
   */
  public boolean start() throws IOException {
    // after StateManager be ready

    // Submit runnable rules to scheduler
    for (RuleContainer container : mapRules.values()) {
        RuleInfo rule = container.getRuleInfoRef();
      if (rule.getState() == RuleState.ACTIVE
          || rule.getState() == RuleState.DRYRUN) {
        submitRuleToScheduler(container.launchExecutor(this));
      }
    }
    return true;
  }

  /**
   * Stop services
   */
  public void stop() throws IOException {
    isClosed = true;
    if (execScheduler != null) {
      execScheduler.shutdown();
    }
  }

  /**
   * Waiting for threads to exit.
   */
  public void join() throws IOException {
  }
}
