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

import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.hdfs.CompatibilityHelperLoader;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.CmdletDescriptor;
import org.smartdata.model.ErasureCodingPolicyInfo;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.rule.RuleExecutorPlugin;
import org.smartdata.model.rule.TranslateResult;
import org.smartdata.server.engine.ServerContext;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ErasureCodingPlugin implements RuleExecutorPlugin {
  private ServerContext context;
  private MetaStore metaStore;
  private final Map<Long, List<String>> ecPolicies = new ConcurrentHashMap<>();
  private final List<ErasureCodingPolicyInfo> ecInfos = new ArrayList<>();
  private long lastUpdateTime = 0;
  private URI nnUri = null;
  private DFSClient client = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(ErasureCodingPlugin.class);

  public ErasureCodingPlugin(ServerContext context) {
    this.context = context;
    metaStore = context.getMetaStore();
    try {
      for (ErasureCodingPolicyInfo info : metaStore.getAllEcPolicies()) {
        ecInfos.add(info);
      }
    } catch (Exception e) {
      // ignore this
      LOG.warn("Load ErasureCoding Policy failed!");
    }
  }


  @Override
  public void onNewRuleExecutor(RuleInfo ruleInfo,
      TranslateResult tResult) {
    long ruleId = ruleInfo.getId();
    CmdletDescriptor des = tResult.getCmdDescriptor();
    for (int i = 0; i < des.getActionSize(); i++) {
      if (des.getActionName(i).equals("ec")) {
        String policy = des.getActionArgs(i).get("-policy");
        if (policy == null) {
          continue;
        }
        if (!ecPolicies.containsKey(ruleId)) {
          ecPolicies.put(ruleId, new ArrayList<String>());
        }
        ecPolicies.get(ruleId).add(policy);
      }
    }
  }

  private void initClient() {
    try {
      if (nnUri == null) {
        nnUri = HadoopUtil.getNameNodeUri(context.getConf());
      }
      if (nnUri != null && client == null) {
        client = HadoopUtil.getDFSClient(nnUri, context.getConf());
      }
    } catch (Exception e) {
      LOG.error("Init client connection failed: " + e.getLocalizedMessage());
    }
  }

  private void updateErasureCodingPolices() {
    try {
      initClient();
      if (client == null) {
        LOG.error("Failed to refresh EC policies due to can not setup connection to HDFS!");
        return;
      }
      Map<Byte, String> idToPolicyName =
          CompatibilityHelperLoader.getHelper().getErasureCodingPolicies(client);
      if (idToPolicyName != null) {
        ecInfos.clear();
        for (Byte id : idToPolicyName.keySet()) {
          ecInfos.add(new ErasureCodingPolicyInfo(id, idToPolicyName.get(id)));
        }
        metaStore.deleteAllEcPolicies();
        metaStore.insertEcPolicies(ecInfos);
      }
    } catch (Exception e) {
      LOG.warn("Failed to refresh EC policies!");
    }
  }

  @Override
  public boolean preExecution(RuleInfo ruleInfo,
      TranslateResult tResult) {
    if (!ecPolicies.containsKey(ruleInfo.getId())) {
      return true;
    }
    List<String> polices = ecPolicies.get(ruleInfo.getId());
    String notIn = null;
    synchronized (ecInfos) {
      for (String policy : polices) {
        notIn = policy;
        for (ErasureCodingPolicyInfo info : ecInfos) {
          if (info.getEcPolicyName().equals(policy)) {
            notIn = null;
            break;
          }
        }
        if (notIn != null) {
          break;
        }
      }
    }

    if (notIn != null) {
      synchronized (ecInfos) {
        long curr = System.currentTimeMillis();
        if (curr - lastUpdateTime >= 5000) {
          LOG.info("Refresh EC policies for policy: " + notIn);
          updateErasureCodingPolices();
          lastUpdateTime = curr;
        }
      }
    }
    return true;
  }

  @Override
  public List<String> preSubmitCmdlet(RuleInfo ruleInfo,
      List<String> objects) {
    return objects;
  }

  @Override
  public CmdletDescriptor preSubmitCmdletDescriptor(RuleInfo ruleInfo,
      TranslateResult tResult, CmdletDescriptor descriptor) {
    return descriptor;
  }

  @Override
  public void onRuleExecutorExit(RuleInfo ruleInfo) {
    ecPolicies.remove(ruleInfo.getId());
  }
}
