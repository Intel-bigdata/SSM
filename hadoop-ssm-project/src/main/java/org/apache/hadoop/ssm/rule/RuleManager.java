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
import org.apache.hadoop.ssm.ModuleSequenceProto;
import org.apache.hadoop.ssm.SSMServer;
import org.apache.hadoop.ssm.rule.parser.RuleStringParser;
import org.apache.hadoop.ssm.rule.parser.TranslateResult;
import org.apache.hadoop.ssm.rule.parser.TranslationContext;
import org.apache.hadoop.ssm.sql.CommandInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.ExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Manage and execute rules.
 * We can have 'cache' here to decrease the needs to execute a SQL query.
 */
public class RuleManager implements ModuleSequenceProto {
  private SSMServer ssm;
  private Configuration conf;
  private DBAdapter dbAdapter;
  private boolean isClosed = false;

  private Map<Long, RuleInfo> mapRules = new HashMap<>();

  private Map<Long, RuleQueryExecutor> mapRuleExecutor = new HashMap<>();

  // TODO: configurable
  public ExecutorScheduler execScheduler = new ExecutorScheduler(4);

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

    mapRules.put(ruleInfo.getId(), ruleInfo);

    if (initState == RuleState.ACTIVE || initState == RuleState.DRYRUN) {
      submitRuleToScheduler(ruleInfo, tr);
    }

    return ruleInfo.getId();
  }

  private TranslateResult doCheckRule(String rule, TranslationContext ctx)
      throws IOException {
    RuleStringParser parser = new RuleStringParser(rule, ctx);
    return parser.translate();
  }

  public void checkRule(String rule) throws IOException {
    doCheckRule(rule, null);
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
    changeRuleState(ruleID, RuleState.DELETED);
  }

  public void ActivateRule(long ruleID) throws IOException {
    RuleInfo info = mapRules.get(ruleID);
    if (info == null) {
      throw new IOException("Rule with ID = " + ruleID + " not found");
    }
    RuleState state = info.getState();
    boolean changed = changeRuleState(ruleID, RuleState.ACTIVE);
    if (changed && state == RuleState.DISABLED) {
      info = mapRules.get(ruleID);
      TranslationContext ctx = new TranslationContext(info.getId(),
          info.getSubmitTime());
      submitRuleToScheduler(info, ctx);
    }
  }

  public void DisableRule(long ruleID, boolean dropPendingCommands)
      throws IOException {
    changeRuleState(ruleID, RuleState.DISABLED);
  }

  public boolean changeRuleState(long ruleID, RuleState newState)
      throws IOException {
    boolean ret = false;
    RuleInfo info = mapRules.get(ruleID);
    if (info == null) {
      throw new IOException("Rule with ID = " + ruleID + " not found");
    }
    if (info.getState() == newState) {
      return false;
    }

    RuleState state = info.getState();
    switch (newState) {
      case ACTIVE:
        if (state == RuleState.DISABLED || state == RuleState.DRYRUN) {
          ret = true;
        }
        break;
      case DISABLED:
        if (state == RuleState.ACTIVE || state == RuleState.DRYRUN) {
          ret = true;
        }
        break;
      case DELETED:
        ret = true;
        break;
      default:
        throw new IOException("Rule state transition " + state
            + " -> " + newState + " is not supported");  // TODO: unsupported
    }

    if (ret) {
      info.setState(newState);
      dbAdapter.updateRuleInfo(info.getId(), newState, 0, 0, 0);
    }
    return true;
  }

  public RuleInfo getRuleInfo(long ruleID) throws IOException {
    RuleInfo info = mapRules.get(ruleID);
    return info == null ? null : info.newCopy();
  }

  public List<RuleInfo> getRuleInfo() throws IOException {
    Collection<RuleInfo> infos = mapRules.values();
    List<RuleInfo> retInfos = new ArrayList<>();
    for (RuleInfo info : infos) {
      retInfos.add(info.newCopy());
    }
    return retInfos;
  }

  public void updateRuleInfo(long ruleId, RuleState rs, long lastCheckTime,
      long checkedCount, int commandsGen) {
    RuleInfo info = mapRules.get(ruleId);
    info.updateRuleInfo(rs, lastCheckTime, checkedCount, commandsGen);
    dbAdapter.updateRuleInfo(ruleId, rs, lastCheckTime,
        checkedCount, commandsGen);
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
      mapRules.put(rule.getId(), rule);
    }
    return true;
  }

  private void submitRuleToScheduler(RuleInfo info,
      TranslationContext transCtx) throws IOException {
    TranslateResult tr = doCheckRule(info.getRuleText(), transCtx);
    submitRuleToScheduler(info, tr);
  }

  private void submitRuleToScheduler(RuleInfo info, TranslateResult tr)
      throws IOException {
    long ruleId = info.getId();
    if (mapRuleExecutor.containsKey(ruleId)) {
      if (!mapRuleExecutor.get(ruleId).isExited()) {
        return;
      }
    }
    ExecutionContext ctx = new ExecutionContext();
    ctx.setProperty(ExecutionContext.RULE_ID, info.getId());
    RuleQueryExecutor qe = new RuleQueryExecutor(this, ctx, tr, dbAdapter);
    mapRuleExecutor.put(ruleId, qe);
    execScheduler.addPeriodicityTask(qe);
  }

  /**
   * Start services
   */
  public boolean start() throws IOException {
    // after StateManager be ready

    // Submit runnable rules to scheduler
    for (RuleInfo rule : mapRules.values()) {
      if (rule.getState() == RuleState.ACTIVE
          || rule.getState() == RuleState.DRYRUN) {
        TranslationContext ctx = new TranslationContext(rule.getId(),
            rule.getSubmitTime());
        submitRuleToScheduler(rule, ctx);
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
