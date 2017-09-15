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

import org.smartdata.model.RuleInfo;

import java.io.IOException;

public interface RulePlugin {

  /**
   * Called when new rule is going to be added to SSM.
   *
   * @param ruleInfo id in ruleInfo is not valid when calling.
   * @param tr
   * @throws IOException the rule won't be added if exception.
   */
  void onAddingNewRule(RuleInfo ruleInfo, TranslateResult tr) throws IOException;

  /**
   * Called when new rule has been added into SSM.
   *
   * @param ruleInfo
   * @param tr
   */
  void onNewRuleAdded(RuleInfo ruleInfo, TranslateResult tr);
}
