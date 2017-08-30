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
package org.smartdata.server.engine.rule;

import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.rule.parser.RuleStringParser;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.rule.parser.TranslationContext;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.data.ExecutionContext;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Contains detailed info about a rule.
 */
public class RuleInfoRepo {
  private RuleInfo ruleInfo = null;
  private RuleExecutor executor = null;
  private MetaStore metaStore = null;

  private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

  public RuleInfoRepo(RuleInfo ruleInfo, MetaStore metaStore) {
    this.ruleInfo = ruleInfo;
    this.metaStore = metaStore;
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

  public void disable() throws IOException {
    lockWrite();
    try {
      changeRuleState(RuleState.DISABLED);
    } finally {
      unlockWrite();
    }
  }

  public void delete() throws IOException {
    lockWrite();
    try {
      changeRuleState(RuleState.DELETED);
    } finally {
      unlockWrite();
    }
  }

  public RuleExecutor activate(RuleManager ruleManager)
      throws IOException {
    lockWrite();
    try {
      changeRuleState(RuleState.ACTIVE);
      return doLaunchExecutor(ruleManager);
    } finally {
      unlockWrite();
    }
  }

  public RuleExecutor launchExecutor(RuleManager ruleManager)
      throws IOException {
    lockWrite();
    try {
      return doLaunchExecutor(ruleManager);
    } finally {
      unlockWrite();
    }
  }

  public boolean updateRuleInfo(RuleState rs, long lastCheckTime,
      long checkedCount, int cmdletsGen) throws IOException {
    lockWrite();
    try {
      boolean ret = true;
      changeRuleState(rs, false);
      ruleInfo.updateRuleInfo(rs, lastCheckTime, checkedCount, cmdletsGen);
      if (metaStore != null) {
        try {
          ret = metaStore.updateRuleInfo(ruleInfo.getId(),
              rs, lastCheckTime, ruleInfo.getNumChecked(), (int)ruleInfo.getNumCmdsGen());
        } catch (MetaStoreException e) {
          throw new IOException(ruleInfo.toString(), e);
        }
      }
      return ret;
    } finally {
      unlockWrite();
    }
  }

  private RuleExecutor doLaunchExecutor(RuleManager ruleManager)
      throws IOException {
    RuleState state = ruleInfo.getState();
    if (state == RuleState.ACTIVE || state == RuleState.DRYRUN) {
      if (executor != null && !executor.isExited()) {
        return null;
      }

      ExecutionContext ctx = new ExecutionContext();
      ctx.setRuleId(ruleInfo.getId());
      TranslationContext transCtx = new TranslationContext(ruleInfo.getId(),
          ruleInfo.getSubmitTime());
      TranslateResult tr = executor != null ? executor.getTranslateResult() :
          new RuleStringParser(ruleInfo.getRuleText(), transCtx).translate();
      executor = new RuleExecutor(
          ruleManager, ctx, tr, ruleManager.getMetaStore());
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

    try {
      switch (newState) {
        case ACTIVE:
          if (oldState == RuleState.DISABLED || oldState == RuleState.DRYRUN) {
            ruleInfo.setState(newState);
            if (updateDb && metaStore != null) {
              metaStore.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
            }
            return true;
          }
          break;

        case DISABLED:
          if (oldState == RuleState.ACTIVE || oldState == RuleState.DRYRUN) {
            ruleInfo.setState(newState);
            markWorkExit();
            if (updateDb && metaStore != null) {
              metaStore.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
            }
            return true;
          }
          break;

        case DELETED:
          ruleInfo.setState(newState);
          markWorkExit();
          if (updateDb && metaStore != null) {
            metaStore.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
          }
          return true;

        case FINISHED:
          if (oldState == RuleState.ACTIVE || oldState == RuleState.DRYRUN) {
            ruleInfo.setState(newState);
            if (updateDb && metaStore != null) {
              metaStore.updateRuleInfo(ruleInfo.getId(), newState, 0, 0, 0);
            }
            return true;
          }
          break;
      }
    } catch (MetaStoreException e) {
      throw new IOException(ruleInfo.toString(), e);
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
