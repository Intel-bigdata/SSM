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

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.smartdata.integration.rest.ActionRestApi.getActionIds;
import static org.smartdata.integration.rest.ActionRestApi.getActionInfoMap;
import static org.smartdata.integration.rest.ActionRestApi.submitAction;
import static org.smartdata.integration.rest.CovUtil.getLong;
import static org.smartdata.integration.rest.RestApiBase.ACTIONROOT;

/**
 * Test for ActionRestApi.
 */
public class TestActionRestApi extends IntegrationTestBase {

  @Test(timeout = 10000)
  public void testActionTypes() {
    Response response = RestAssured.get(ACTIONROOT + "/registry/list");
    ValidatableResponse validatableResponse = response.then().root("body");
    validatableResponse.body(
        "find { it.actionName == 'allssd' }.displayName", Matchers.equalTo("allssd"));
    validatableResponse.body(
        "actionName",
        Matchers.hasItems(
            "uncache",
            "write",
            "cache",
            "read",
            "allssd",
            "checkstorage",
            "archive",
            "onessd",
            "echo"));
  }

  @Test(timeout = 200000)
  public void testActionsInSequence() throws Exception {
    // write and read
    testAction("write", "-file /hello -length 10");
    testAction("read", "-file /hello");
    Map checkStorage1 = testAction("checkstorage", "-file /hello");
    String result1 = (String) checkStorage1.get("result");
    Assert.assertEquals(3, countSubstring(result1, "DISK"));

    // move to all ssd
    testAction("allssd", "-file /hello");
    Map checkStorage2 = testAction("checkstorage", "-file /hello");
    String result2 = (String) checkStorage2.get("result");
    Assert.assertEquals(3, countSubstring(result2, "SSD"));

    // move to archive
    testAction("archive", "-file /hello");
    Map checkStorage3 = testAction("checkstorage", "-file /hello");
    String result3 = (String) checkStorage3.get("result");
    Assert.assertEquals(3, countSubstring(result3, "ARCHIVE"));

    // move to one ssd
    testAction("onessd", "-file /hello");
    Map checkStorage4 = testAction("checkstorage", "-file /hello");
    String result4 = (String) checkStorage4.get("result");
    Assert.assertEquals(2, countSubstring(result4, "DISK"));
    Assert.assertEquals(1, countSubstring(result4, "SSD"));

    // move to cache
    testAction("cache", "-file /hello");
  }

/*
  @Test
  public void testDistributedAction() throws Exception {
    Process worker = Util.startNewServer();
    try {
      Process agent = Util.startNewAgent();
      try {
        Util.waitSlaveServerAvailable();
        Util.waitAgentAvailable();

        // Three actions would be executed on Master, StandbyServer and Agent
        testAction("hello", "-print_message message");
        testAction("hello", "-print_message message");
        testAction("hello", "-print_message message");
      } finally {
        agent.destroy();
      }
    } finally {
      worker.destroy();
    }
  }
*/

  private int countSubstring(String parent, String child) {
    Pattern storagePattern = Pattern.compile(child);
    Matcher matcher = storagePattern.matcher(parent);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }

  // submit an action and wait until it is finished with some basic info check
  private Map testAction(String actionType, String args) throws Exception {
    // add a write action by submitting cmdlet
    Long cid = submitAction(actionType, args);

    Long aid;
    Map actionInfoMap;
    // check action info until the action is finished
    while (true) {
      Thread.sleep(1000);
      System.out.println("Action " + actionType + " is running...");
      // get aid from cmdletInfo
      aid = getActionIds(cid).get(0);

      // get actionInfo
      actionInfoMap = getActionInfoMap(aid);
      Assert.assertEquals(actionType, actionInfoMap.get("actionName"));
      Assert.assertEquals(aid, getLong(actionInfoMap.get("actionId")));
      Assert.assertEquals(cid, getLong(actionInfoMap.get("cmdletId")));
      Boolean finished = (Boolean) actionInfoMap.get("finished");
      if (finished) {
        Assert.assertEquals(true, actionInfoMap.get("successful"));
        Assert.assertEquals(1.0f, (float) actionInfoMap.get("progress"), 0.000001f);
        break;
      }
    }

    // check action list
 /*   Response actionList = RestAssured.get(ACTIONROOT + "/list/0");
    actionList.then().body("status", Matchers.equalTo("OK"));
    actionList.jsonPath().getList("body.actionId", Long.class).contains(aid);

    // check action type list
    actionList = RestAssured.get(ACTIONROOT + "/type/0/" + actionType);
    actionList.then().body("status", Matchers.equalTo("OK"));
    actionList.jsonPath().getList("body.actionId", Long.class).contains(aid);

    System.out.println("Action " + actionType + " is finished.");*/
    return actionInfoMap;
  }
}
