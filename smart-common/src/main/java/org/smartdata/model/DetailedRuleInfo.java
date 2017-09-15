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
package org.smartdata.model;

public class DetailedRuleInfo extends RuleInfo {
  public long baseProgress;
  public long runningProgress;

  public DetailedRuleInfo(RuleInfo ruleInfo) {
    // Init from ruleInfo
    super(ruleInfo.getId(), ruleInfo.getSubmitTime(), ruleInfo.getRuleText(), ruleInfo.getState(),
        ruleInfo.getNumChecked(), ruleInfo.getNumCmdsGen(), ruleInfo.getLastCheckTime());
  }

  public long getBaseProgress() {
    return baseProgress;
  }

  public void setBaseProgress(long baseProgress) {
    this.baseProgress = baseProgress;
  }

  public long getRunningProgress() {
    return runningProgress;
  }

  public void setRunningProgress(long runningProgress) {
    this.runningProgress = runningProgress;
  }


}
