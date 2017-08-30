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
package org.smartdata.server.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.action.ActionRegistry;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.rule.parser.RuleStringParser;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.rule.parser.TranslationContext;
import org.smartdata.server.engine.rule.ExecutorScheduler;
import org.smartdata.server.engine.rule.RuleExecutor;
import org.smartdata.server.engine.rule.RuleInfoRepo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage and execute rules.
 * We can have 'cache' here to decrease the needs to execute a SQL query.
 */
public class RuleManager extends AbstractService {
  private ServerContext serverContext;
  private StatesManager statesManager;
  private CmdletManager cmdletManager;
  private MetaStore metaStore;

  private boolean isClosed = false;
  public static final Logger LOG =
      LoggerFactory.getLogger(RuleManager.class.getName());

  private ConcurrentHashMap<Long, RuleInfoRepo> mapRules =
      new ConcurrentHashMap<>();

  public ExecutorScheduler execScheduler;

  public RuleManager(ServerContext context,
      StatesManager statesManager, CmdletManager cmdletManager) {
    super(context);

    int numExecutors = context.getConf().getInt(
        SmartConfKeys.SMART_RULE_EXECUTORS_KEY,
        SmartConfKeys.SMART_RULE_EXECUTORS_DEFAULT);
    execScheduler = new ExecutorScheduler(numExecutors);

    this.statesManager = statesManager;
    this.cmdletManager = cmdletManager;
    this.serverContext = context;
    this.metaStore = context.getMetaStore();
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
    LOG.debug("Received Rule -> [" + rule + "]");
    if (initState != RuleState.ACTIVE && initState != RuleState.DISABLED
        && initState != RuleState.DRYRUN) {
      throw new IOException("Invalid initState = " + initState
          + ", it MUST be one of [" + RuleState.ACTIVE
          + ", " + RuleState.DRYRUN + ", " + RuleState.DISABLED + "]");
    }

    TranslateResult tr = doCheckRule(rule, null);
    doCheckActions(tr.getCmdDescriptor());

    RuleInfo.Builder builder = RuleInfo.newBuilder();
    builder.setRuleText(rule).setState(initState);
    RuleInfo ruleInfo = builder.build();
    try {
      metaStore.insertNewRule(ruleInfo);
    } catch (MetaStoreException e) {
      throw new IOException("RuleText = " + rule, e);
    }

    RuleInfoRepo infoRepo = new RuleInfoRepo(ruleInfo, metaStore);
    mapRules.put(ruleInfo.getId(), infoRepo);
    submitRuleToScheduler(infoRepo.launchExecutor(this));

    return ruleInfo.getId();
  }

  private void doCheckActions(CmdletDescriptor cd) throws IOException {
    String error = "";
    for (int i = 0; i < cd.actionSize(); i++) {
      if (!ActionRegistry.registeredAction(cd.getActionName(i))) {
        error += "Action '" + cd.getActionName(i) + "' not supported.\n";
      }
    }
    if (error.length() > 0) {
      throw new IOException(error);
    }
  }

  private TranslateResult doCheckRule(String rule, TranslationContext ctx)
      throws IOException {
    RuleStringParser parser = new RuleStringParser(rule, ctx);
    return parser.translate();
  }

  public void checkRule(String rule) throws IOException {
    doCheckRule(rule, null);
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * Delete a rule in SSM. if dropPendingCmdlets equals false then the rule
   * record will still be kept in Table 'rules', the record will be deleted
   * sometime later.
   *
   * @param ruleID
   * @param dropPendingCmdlets pending cmdlets triggered by the rule will be
   *                            discarded if true.
   * @throws IOException
   */
  public void deleteRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    RuleInfoRepo infoRepo = checkIfExists(ruleID);
    try {
      if (dropPendingCmdlets && getCmdletManager() != null) {
        getCmdletManager().deleteCmdletByRule(infoRepo.getRuleInfo().getId());
      }
    } finally {
      infoRepo.delete();
    }
  }

  public void activateRule(long ruleID) throws IOException {
    RuleInfoRepo infoRepo = checkIfExists(ruleID);
    submitRuleToScheduler(infoRepo.activate(this));
  }

  public void disableRule(long ruleID, boolean dropPendingCmdlets)
      throws IOException {
    RuleInfoRepo infoRepo = checkIfExists(ruleID);
    infoRepo.disable();
  }

  private RuleInfoRepo checkIfExists(long ruleID) throws IOException {
    RuleInfoRepo infoRepo = mapRules.get(ruleID);
    if (infoRepo == null) {
      throw new IOException("Rule with ID = " + ruleID + " not found");
    }
    return infoRepo;
  }

  public RuleInfo getRuleInfo(long ruleID) throws IOException {
    RuleInfoRepo infoRepo = checkIfExists(ruleID);
    return infoRepo.getRuleInfo();
  }

  public List<RuleInfo> listRulesInfo() throws IOException {
    Collection<RuleInfoRepo> infoRepos = mapRules.values();
    List<RuleInfo> retInfos = new ArrayList<>();
    for (RuleInfoRepo infoRepo : infoRepos) {
      retInfos.add(infoRepo.getRuleInfo());
    }
    return retInfos;
  }

  public void updateRuleInfo(long ruleId, RuleState rs, long lastCheckTime,
                             long checkedCount, int cmdletsGen) throws IOException {
    RuleInfoRepo infoRepo = checkIfExists(ruleId);
    infoRepo.updateRuleInfo(rs, lastCheckTime, checkedCount, cmdletsGen);
  }

  public boolean isClosed() {
    return isClosed;
  }

  public StatesManager getStatesManager() {
    return statesManager;
  }

  public CmdletManager getCmdletManager() {
    return cmdletManager;
  }

  /**
   * Init RuleManager, this includes:
   *    1. Load related data from local storage or HDFS
   *    2. Initial
   * @throws IOException
   */
  @Override
  public void init() throws IOException {
    LOG.info("Initializing ...");
    // Load rules table
    List<RuleInfo> rules = null;
    try {
      rules = metaStore.getRuleInfo();
    } catch (MetaStoreException e) {
      LOG.error("Can not load rules from database:\n" + e.getMessage());
    }
    for (RuleInfo rule : rules) {
      mapRules.put(rule.getId(), new RuleInfoRepo(rule, metaStore));
    }
    LOG.info("Initialized. Totally " + rules.size()
        + " rules loaded from DataBase.");
    if (LOG.isDebugEnabled()) {
      for (RuleInfo info : rules) {
        LOG.debug("\t" + info);
      }
    }
  }

  private boolean submitRuleToScheduler(RuleExecutor executor)
      throws IOException {
    if (executor == null || executor.isExited()) {
      return false;
    }
    execScheduler.addPeriodicityTask(executor);
    return true;
  }

  /**
   * Start services
   */
  @Override
  public void start() throws IOException {
    LOG.info("Starting ...");
    // after StateManager be ready

    int numLaunched = 0;
    // Submit runnable rules to scheduler
    for (RuleInfoRepo infoRepo : mapRules.values()) {
      RuleInfo rule = infoRepo.getRuleInfoRef();
      if (rule.getState() == RuleState.ACTIVE
          || rule.getState() == RuleState.DRYRUN) {
        boolean sub = submitRuleToScheduler(infoRepo.launchExecutor(this));
        numLaunched += sub ? 1 : 0;
      }
    }
    LOG.info("Started. " + numLaunched + " rules launched for execution.");
  }

  /**
   * Stop services
   */
  @Override
  public void stop() throws IOException {
    LOG.info("Stopping ...");
    isClosed = true;
    if (execScheduler != null) {
      execScheduler.shutdown();
    }
    LOG.info("Stopped.");
  }
}
