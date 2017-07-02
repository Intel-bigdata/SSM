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
import org.smartdata.integration.util.RetryTask;
import org.smartdata.integration.util.Util;
import org.smartdata.server.SmartDaemon;
import org.smartdata.server.engine.StandbyServerInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static io.restassured.path.json.JsonPath.with;

/**
 * Test for ClusterRestApi.
 */
public class TestClusterRestApi extends IntegrationTestBase {
  @Test
  public void testPrimary() {
    Response response = RestAssured.get("/smart/api/v1/cluster/primary");
    String json = response.asString();
    response.then().body("message", Matchers.equalTo("Namenode URL"));
    response.then().body("body", Matchers.containsString("localhost"));
  }

  @Test
  public void testServers() throws IOException, InterruptedException {
    Response response = RestAssured.get("/smart/api/v1/cluster/servers");
    response.then().body("body", Matchers.empty());
    Process worker = Util.startNewServer();
    Process agent = Util.startNewAgent();

    Util.waitSlaveServerAvailable();
    Util.waitAgentAvailable();

    agent.destroy();
    worker.destroy();
  }
}
