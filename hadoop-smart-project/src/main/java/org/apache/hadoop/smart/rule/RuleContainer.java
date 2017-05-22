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
package org.apache.hadoop.smart.rule;

import org.apache.hadoop.smart.rule.parser.TranslateResult;
import org.apache.hadoop.smart.rule.parser.TranslationContext;
import org.apache.hadoop.smart.sql.DBAdapter;
import org.apache.hadoop.smart.sql.ExecutionContext;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RuleContainer {
  private RuleInfo ruleInfo = null;
  private RuleQueryExecutor executor = null;
  private DBAdapter dbAdapter = null;

  private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

  public RuleContainer(RuleInfo ruleInfo, DBAdapter dbAdapter) {
    this.ruleInfo = ruleInfo;
    this.dbAdapter = dbAdapter;
  }

  public RuleInfo getRuleInfo() {
    lockRead();
    RuleInfo ret = ruleInfo.newCopy();
    unlockRead();
    return ret;
  }

  public RuleInfo getRuleInfoRef() {
    return ruleInfo;
  }

  public void DisableRule() throws IOException {
    lockWrite();
    try {
      changeRuleState(RuleState.DISABLED);
    } finally {
      unlockWrite();
    }
  }

  public void DeleteRule() throws IOException {
    lockWrite();
    try {
      changeRuleState(RuleState.DELETED);
    } finally {
      unlockWrite();
    }
  }

  public RuleQueryExecutor ActivateRule(RuleManager ruleManager)
      throws IOException {
    lockWrite();
    try {
      changeRuleState(RuleState.ACTIVE);
      return doLaunchExecutor(ruleManager);
    } finally {
      unlockWrite();
    }
  }

  public RuleQueryExecutor launchExecutor(RuleManager ruleManager)
      throws IOException {
    lockWrite();
    try {
      return doLaunchExecutor(ruleManager);
    } finally {
      unlockWrite();
    }
  }

  public boolean updateRuleInfo(RuleState rs, long lastCheckTime,
      long checkedCount, int commandsGen) throws IOException {
    lockWrite();
    try {
      boolean ret = true;
      changeRuleState(rs, false);
      ruleInfo.updateRuleInfo(rs, lastCheckTime, checkedCount, commandsGen);
      if (dbAdapter != null) {
        ret = dbAdapter.updateRuleInfo(ruleInfo.getId(),
            rs, lastCheckTime, checkedCount, commandsGen);
      }
      return ret;
    } finally {
      unlockWrite();
    }
  }

  private RuleQueryExecutor doLaunchExecutor(RuleManager ruleManager)
      throws IOException {
    RuleState state = ruleInfo.getState();
    if (state == RuleState.ACTIVE || state == RuleState.DRYRUN) {
      if (executor != null && !executor.isExited()) {
        return null;
      }

      ExecutionContext ctx = new ExecutionContext();
      ctx.setProperty(ExecutionContext.RULE_ID, ruleInfo.getId());
      TranslationContext transCtx = new TranslationContext(ruleInfo.getId(),
          ruleInfo.getSubmitTime());
      TranslateResult tr = executor != null ? executor.getTranslateResult() :
          ruleManager.doCheckRule(ruleInfo.getRuleText(), transCtx);
      executor = new RuleQueryExecutor(
          ruleManager, ctx, tr, ruleManager.getDbAdapter());
      return executor;
    }
    return null;
  }

  private void markWorkExit() {
    if (executor != null) {
      executor.setExited();
      //System.out.println(executor + " -> disabled");
    }
  }

  private boolean changeRuleState(RuleState newState)
      throws IOException {
    return changeRuleState(newState, true);
  }

  private boolean changeRuleState(RuleState newState,
      boolean updateDb) throws IOException {
    RuleState oldState = ruleInfo.getState();
    if (newState == null || oldState == newState) {
      return false;
    }

    switch (newState) {
      case ACTIVE:
        if (oldState == RuleState.DISABLED || oldState == RuleState.DRYRUN) {
          ruleInfo.setState(newState);
          if (updateDb && dbAdapter != null) {
            dbAdapter.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
          }
          return true;
        }
        break;

      case DISABLED:
        if (oldState == RuleState.ACTIVE || oldState == RuleState.DRYRUN) {
          ruleInfo.setState(newState);
          markWorkExit();
          if (updateDb && dbAdapter != null) {
            dbAdapter.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
          }
          return true;
        }
        break;

      case DELETED:
        ruleInfo.setState(newState);
        markWorkExit();
        if (updateDb && dbAdapter != null) {
          dbAdapter.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
        }
        return true;

      case FINISHED:
        if (oldState == RuleState.ACTIVE || oldState == RuleState.DRYRUN) {
          ruleInfo.setState(newState);
          if (updateDb && dbAdapter != null) {
            dbAdapter.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
          }
          return true;
        }
        break;
    }

    throw new IOException("Rule state transition " + oldState
        + " -> " + newState + " is not supported");  // TODO: unsupported
  }

  private void lockWrite() {
    rwl.writeLock().lock();
  }

  private void unlockWrite() {
    rwl.writeLock().unlock();
  }

  private void lockRead() {
    rwl.readLock().lock();
  }

  private void unlockRead() {
    rwl.readLock().unlock();
  }
}
