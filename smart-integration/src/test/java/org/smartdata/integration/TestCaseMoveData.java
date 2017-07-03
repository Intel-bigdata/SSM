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
package org.smartdata.integration;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.integration.rest.RuleRestApi;

import static org.smartdata.integration.rest.ActionRestApi.getActionInfo;
import static org.smartdata.integration.rest.CmdletRestApi.getCmdletActionIds;
import static org.smartdata.integration.rest.CmdletRestApi.submitCmdlet;
import static org.smartdata.integration.rest.CmdletRestApi.waitCmdletComplete;
import static org.smartdata.integration.rest.RuleRestApi.startRule;
import static org.smartdata.integration.rest.RuleRestApi.waitRuleTriggered;

public class TestCaseMoveData extends IntegrationTestBase {

  @Test(timeout = 120000)
  public void testOneSsdHotData() throws Exception {
    String file = "/testOneSsd/testOneSsdFile";
    waitCmdletComplete(submitCmdlet("write -length 1024 -file " + file));
    waitCmdletComplete(submitCmdlet("archive -file " + file));
    Assert.assertTrue(checkStorage(file, "ARCHIVE", "SSD"));
    
    String rule = "file : every 1s | path matches \"/testOneSsd/*\" and accessCount(1min) > 1 | onessd";
    long ruleId = RuleRestApi.submitRule(rule);
    startRule(ruleId);

    waitCmdletComplete(submitCmdlet("read -file " + file));
    waitCmdletComplete(submitCmdlet("read -file " + file));

    waitRuleTriggered(ruleId);

    while (!checkStorage(file, "SSD", "ARCHIVE")) {
      Thread.sleep(1000);
    }
  }

  @Test(timeout = 120000)
  public void testArchiveColdData() throws Exception {
    String file = "/testArchive/testArchiveFile";
    waitCmdletComplete(submitCmdlet("write -length 1024 -file " + file));
    Assert.assertTrue(checkStorage(file, null, "ARCHIVE"));


    String rule = "file : every 1s | path matches \"/testArchive/*\" and age > 10s | archive";
    long ruleId = RuleRestApi.submitRule(rule);
    startRule(ruleId);

    waitRuleTriggered(ruleId);

    while (!checkStorage(file, "ARCHIVE", null)) {
      Thread.sleep(1000);
    }
  }

  private boolean checkStorage(String file,
      String containStorageType, String notContainStorageType) {
    long cmdletChkArchive = submitCmdlet("checkstorage -file " + file);
    waitCmdletComplete(cmdletChkArchive);
    String result = getActionInfo(getCmdletActionIds(cmdletChkArchive).get(0))
        .getString("result");
    return checkStorageResult(result, containStorageType, notContainStorageType);
  }

  private boolean checkStorageResult(String result,
      String containStorageType, String notContainStorageType) {
    if (containStorageType != null) {
      if (!containStorageType.startsWith("[")) {
        containStorageType = "[" + containStorageType + "]";
      }
      if (!result.contains(containStorageType)) {
        return false;
      }
    }

    if (notContainStorageType != null) {
      if (!notContainStorageType.startsWith("[")) {
        notContainStorageType = "[" + notContainStorageType + "]";
      }
      if (result.contains(notContainStorageType)) {
        return false;
      }
    }
    return true;
  }
}