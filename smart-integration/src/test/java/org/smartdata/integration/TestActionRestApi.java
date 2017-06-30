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
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test for ActionRestApi.
 */
public class TestActionRestApi extends IntegrationTestBase {
  private static final String ACTION_ROOT = "/smart/api/v1/actions";
  private static final String CMDLET_ROOT = "/smart/api/v1/cmdlets";

  @Test
  public void testActionTypes() {
    Response response = RestAssured.get(ACTION_ROOT + "/registry/list");
    ValidatableResponse validatableResponse = response.then().root("body");
    validatableResponse.body("find { it.actionName == 'fsck' }.displayName",
        Matchers.equalTo("fsck"));
    validatableResponse.body("actionName", Matchers.hasItems("fsck",
        "diskbalance", "uncache", "setstoragepolicy", "blockec", "copy",
        "write", "stripec", "cache", "read", "allssd", "checkstorage",
        "archive", "list", "clusterbalance", "onessd", "hello"));
  }

  @Test (timeout = 300000)
  public void testActionsInSequence() throws Exception {
    // write and read
    testAction("write", "-file /hello -length 10");
    testAction("read", "-file /hello");
    Map checkStorage1 = testAction("checkstorage", "-file /hello");
    String result1 = (String)checkStorage1.get("result");
    Assert.assertEquals(3, countSubstring(result1, "DISK"));

    // move to all ssd
    testAction("allssd", "-file /hello");
    Map checkStorage2 = testAction("checkstorage", "-file /hello");
    String result2 = (String)checkStorage2.get("result");
    Assert.assertEquals(3, countSubstring(result2, "SSD"));

    // move to archive
    testAction("archive", "-file /hello");
    Map checkStorage3 = testAction("checkstorage", "-file /hello");
    String result3 = (String)checkStorage3.get("result");
    Assert.assertEquals(3, countSubstring(result3, "ARCHIVE"));

    // move to one ssd
    testAction("onessd", "-file /hello");
    Map checkStorage4 = testAction("checkstorage", "-file /hello");
    String result4 = (String)checkStorage4.get("result");
    Assert.assertEquals(2, countSubstring(result4, "DISK"));
    Assert.assertEquals(1, countSubstring(result4, "SSD"));

    // move to cache
    testAction("cache", "-file /hello");
  }

  private int countSubstring(String parent, String child) {
    Pattern storagePattern = Pattern.compile(child);
    Matcher matcher = storagePattern.matcher(parent);
    int count = 0;
    while(matcher.find()) {
      count ++;
    }
    return count;
  }

  // submit an action using action type and arguments
  private int submitAction(String actionType, String args) {
    Response action = RestAssured.post(CMDLET_ROOT +
        "/submit/" + actionType + "?" + "args=" + args);
    action.then().body("status", Matchers.equalTo("CREATED"));
    return new JsonPath(action.asString()).getInt("body");
  }

  // get aids of a cmdlet
  private List<Integer> getActionIds(int cid) {
    Response cmdletInfo = RestAssured.get(CMDLET_ROOT + "/" + cid + "/info");
    JsonPath cmdletInfoPath = new JsonPath(cmdletInfo.asString());
    return (List)cmdletInfoPath.getMap("body").get("aids");
  }

  // get action info
  private Map getActionInfo(int aid) {
    Response actionInfo = RestAssured.get(ACTION_ROOT + "/" + aid + "/info");
    JsonPath actionInfoPath = new JsonPath(actionInfo.asString());
    Map actionInfoMap = actionInfoPath.getMap("body");
    return actionInfoMap;
  }

  // submit an action and wait until it is finished with some basic info check
  private Map testAction(String actionType, String args) throws Exception {
    // add a write action by submitting cmdlet
    int cid = submitAction(actionType, args);

    int aid;
    Map actionInfoMap;
    // check action info until the action is finished
    while (true) {
      Thread.sleep(1000);
      System.out.println("Action " + actionType + " is running...");
      // get aid from cmdletInfo
      aid = getActionIds(cid).get(0);

      // get actionInfo
      actionInfoMap = getActionInfo(aid);
      Assert.assertEquals(actionType, actionInfoMap.get("actionName"));
      Assert.assertEquals(aid, actionInfoMap.get("actionId"));
      Assert.assertEquals(cid, actionInfoMap.get("cmdletId"));
      Boolean finished = (Boolean)actionInfoMap.get("finished");
      if (finished) {
        Assert.assertEquals(true, actionInfoMap.get("successful"));
        Assert.assertEquals(1.0f, (float)actionInfoMap.get("progress"), 0.000001f);
        break;
      }
    }

    // check action list
    Response actionList = RestAssured.get(ACTION_ROOT + "/list/0");
    actionList.then().body("status", Matchers.equalTo("OK"))
        .root("body").body("actionId", Matchers.hasItems(aid));
    System.out.println("Action " + actionType + " is finished.");
    return actionInfoMap;
  }
}
