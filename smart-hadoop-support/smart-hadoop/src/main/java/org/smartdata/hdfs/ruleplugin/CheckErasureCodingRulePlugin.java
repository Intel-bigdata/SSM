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
package org.smartdata.hdfs.ruleplugin;

import org.smartdata.metastore.MetaStore;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RulePlugin;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.versioninfo.SsmVersionInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CheckErasureCodingRulePlugin implements RulePlugin {
  private MetaStore metaStore;
  private static final List<String> ecActions = Arrays.asList("ec", "unec", "checkec", "listec");

  public CheckErasureCodingRulePlugin(MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public void onAddingNewRule(RuleInfo ruleInfo, TranslateResult tr)
      throws IOException {
    boolean ecRelated = false;
    for (String act : tr.getCmdDescriptor().getActionNames()) {
      if (ecActions.contains(act)) {
        ecRelated = true;
        break;
      }
    }

    if (ecRelated && SsmVersionInfo.getHadoopVersionMajor() <= 2) {
      throw new IOException("ErasureCoding feature is not supported in Hadoop "
          + SsmVersionInfo.getHadoopVersion());
    }
  }

  public void onNewRuleAdded(RuleInfo ruleInfo, TranslateResult tr) {
  }
}