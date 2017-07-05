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

import org.junit.Assert;
import org.junit.Test;


public class TestRuleInfo {
  private class ChildRuleInfo extends RuleInfo {

  }

  @Test
  public void testEquals() throws Exception {
    //Case 1:
    Assert.assertEquals(true, new RuleInfo().equals(new RuleInfo()));

    //Case 2:
    RuleInfo ruleInfo = new RuleInfo(1, 1, "", RuleState.ACTIVE, 1, 1, 1);
    Assert.assertEquals(true, ruleInfo.equals(ruleInfo));

    //Case 3:
    RuleInfo ruleInfo1 = new RuleInfo(1, 1, "", null, 1, 1, 1);
    Assert.assertEquals(false, ruleInfo.equals(ruleInfo1));
    Assert.assertEquals(false, ruleInfo1.equals(ruleInfo));

    //Case 4:
    RuleInfo ruleInfo2 = new RuleInfo(1, 1, null, RuleState.ACTIVE, 1, 1, 1);
    Assert.assertEquals(false, ruleInfo.equals(ruleInfo2));
    Assert.assertEquals(false, ruleInfo2.equals(ruleInfo));
  }
}
