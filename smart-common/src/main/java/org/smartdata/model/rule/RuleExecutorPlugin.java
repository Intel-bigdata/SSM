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
package org.smartdata.model.rule;

import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.RuleInfo;

import java.util.List;

public interface RuleExecutorPlugin {

  /**
   * Called just before an RuleExecutor been generated and submitted to
   * thread pool for execution. Only called once per instance.
   *
   * @param ruleInfo
   * @param tResult
   */
  void onNewRuleExecutor(final RuleInfo ruleInfo, TranslateResult tResult);

  /**
   * Called just before rule executor begin to execute rule.
   *
   * @param ruleInfo
   * @param tResult
   * @return continue this execution if true.
   */
  boolean preExecution(final RuleInfo ruleInfo, TranslateResult tResult);

  /**
   * Called after rule condition checked.
   *
   * @param objects the result of checking rule condition.
   * @return object list that will be used for Cmdlet submission.
   */
  List<String> preSubmitCmdlet(final RuleInfo ruleInfo, List<String> objects);

  /**
   * Called right before the CmdletDescriptor been submitted to CmdletManager.
   *
   * @param descriptor
   * @return the descriptor that will be used to submit to CmdletManager
   */
  CmdletDescriptor preSubmitCmdletDescriptor(final RuleInfo ruleInfo, TranslateResult tResult,
      CmdletDescriptor descriptor);

  /**
   * Called when an RuleExecutor exits. Called only once per instance.
   *
   * @param ruleInfo
   */
  void onRuleExecutorExit(final RuleInfo ruleInfo);
}
