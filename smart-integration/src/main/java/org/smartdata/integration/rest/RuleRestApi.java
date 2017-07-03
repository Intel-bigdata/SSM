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
package org.smartdata.integration.rest;

import io.restassured.RestAssured;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import org.hamcrest.Matchers;

import static org.hamcrest.Matchers.equalTo;

public class RuleRestApi extends RestApiBase {

  public static long submitRule(String ruleText) {
    Response res = RestAssured.with()
        .body("ruleText=" + ruleText).post(RULEROOT + "/add");
    res.then().body("status", equalTo("CREATED"));
    return res.jsonPath().getLong("body");
  }

  public static void startRule(long ruleId) {
    RestAssured.post(RULEROOT + "/" + ruleId + "/start").then()
        .body("status", equalTo("OK"));
  }

  public static JsonPath getRuleInfo(long ruleId) {
    Response cmdletInfo = RestAssured.get(RULEROOT + "/" + ruleId + "/info");
    cmdletInfo.then().body("status", Matchers.equalTo("OK"));
    JsonPath path = cmdletInfo.jsonPath().setRoot("body");
    return path;
  }

  /**
   * Wait until the specified rule generates one cmdlet.
   * @param ruleId
   * @param secToWait
   * @return
   */
  public static boolean waitRuleTriggered(long ruleId, int secToWait) {
    do {
      JsonPath pa = getRuleInfo(ruleId);
//      System.out.println("numChecked = " + pa.getLong("numChecked")
//          + ", numCmdsGen = " + pa.getLong("numCmdsGen"));
      if (pa.getLong("numCmdsGen") > 0) {
        return true;
      }
      secToWait--;
      if (secToWait < 0) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        // ignore
      }
    } while (true);
    return false;
  }

  public static boolean waitRuleTriggered(long ruleId) {
    return waitRuleTriggered(ruleId, Integer.MAX_VALUE);
  }
}
