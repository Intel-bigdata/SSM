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
import org.smartdata.integration.rest.RestApiBase;

/**
 * Test for SystemRestApi.
 */
public class TestSystemRestApi extends IntegrationTestBase {

  @Test
  public void testVersion() throws Exception {
    Response response1 = RestAssured.get(RestApiBase.SYSTEMROOT + "/version");
    String json1 = response1.asString();
    response1.then().body("body", Matchers.equalTo("1.4.0"));
  }

//  @Test
//  public void testServers() throws IOException, InterruptedException {
//    Response response = RestAssured.get(RestApiBase.SYSTEMROOT + "/servers");
//    response.then().body("body", Matchers.empty());
//    Process worker = null;
//    Process agent = null;
//
//    try {
//      worker = Util.startNewServer();
//      Util.waitSlaveServerAvailable();
//
//      agent = Util.startNewAgent();
//      Util.waitAgentAvailable();
//    } finally {
//      if (worker != null) {
//        try {
//          worker.destroy();
//          Util.waitSlaveServersDown();
//        } catch (Throwable t) {
//          // ignore
//        }
//      }
//
//      if (agent != null) {
//        try {
//          agent.destroy();
//          Util.waitAgentsDown();
//        } catch (Throwable t) {
//          // ignore
//        }
//      }
//    }
//  }
}
