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
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TestRuleRestApi extends IntegrationTestBase {

  @Test
  public void test() throws Exception {
    String rule = "file : every 1s | path matches \"/home/test\" and age > 2m | archive";
    //Response res = RestAssured.post(RULEROOT + "/add/" + rule);
    Response res = RestAssured.with().body("ruleText=" + rule).post(RULEROOT + "/add/");
    res.then().body("status", equalTo("CREATED"));
    long ruleId = res.jsonPath().getLong("body");

    Thread.sleep(1000);
    RestAssured.get(RULEROOT + "/list").then().body("status", equalTo("OK"))
        .body("body.size", is(1)).root("body").body("ruleText", contains(rule))
        .body("numChecked", contains(0));

    RestAssured.post(RULEROOT + "/" + ruleId + "/start").then()
        .body("status", equalTo("OK"));
    Thread.sleep(2000);
    RestAssured.get(RULEROOT + "/" + ruleId + "/info").then()
        .body("body.numChecked", Matchers.greaterThan(0));

    RestAssured.post(RULEROOT + "/" + ruleId + "/stop").then()
        .body("status", equalTo("OK"));

    RestAssured.get(RULEROOT + "/" + ruleId + "/info").then()
        .body("body.state", equalTo("DISABLED"))
        .body("body.numCmdsGen", is(0));

    RestAssured.get(RULEROOT + "/" + ruleId + "/cmdlets").then()
        .body("status", equalTo("OK"))
        .body("body.size", is(0));

    RestAssured.post(RULEROOT + "/" + ruleId + "/delete").then()
        .body("status", equalTo("OK"));
  }
}
