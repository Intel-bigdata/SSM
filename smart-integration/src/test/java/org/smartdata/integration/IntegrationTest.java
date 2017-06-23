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
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.integration.cluster.MiniSmartCluster;
import org.smartdata.integration.cluster.SmartCluster;


/**
 * Integration test.
 */
public class IntegrationTest {
  private static SmartCluster cluster;
  private static SmartConf conf;
  private static IntegrationSmartServer smartServer;
  private static String httpUri;
  private static String httpHost;
  private static int httpPort;

  @BeforeClass
  public static void setup() throws Exception {
    // Set up an HDFS cluster
    cluster = new MiniSmartCluster();
    cluster.setUp();

    // Start a Smart server
    conf = cluster.getConf();
    httpHost = "localhost";
    httpPort = 7045;
    httpUri = httpHost + ":" + httpPort;
    conf.set(SmartConfKeys.DFS_SSM_HTTP_ADDRESS_KEY, httpUri);
    smartServer = new IntegrationSmartServer();
    smartServer.setUp(conf);

    // Initialize RestAssured
    initRestAssured();
  }

  private static void initRestAssured() {
    RestAssured.port = httpPort;
    RestAssured.registerParser("text/plain", Parser.JSON);
  }

  // Just an example
  @Test
  public void testSubmitAction() throws Exception {
    Response response0 = RestAssured.get("/api/v1.0/actionlist");
    String json0 = response0.asString();
    RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello -length 10");
    Thread.sleep(2000);
    RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello2 -length 10");
    Thread.sleep(2000);
    RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello3 -length 10");
    Thread.sleep(2000);
    RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello4 -length 10");
    Thread.sleep(2000);
    RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello5 -length 10");

    Thread.sleep(5000);
    Response response = RestAssured.get("/api/v1.0/actionlist");
    String json = response.asString();
    System.out.print(json);

    /*response.then().body("actionId[0]", Matchers.equalTo(5))
        .body("actionId[1]", Matchers.equalTo(4))
        .body("actionId[2]", Matchers.equalTo(3))
        .body("actionId[3]", Matchers.equalTo(2))
        .body("actionId[4]", Matchers.equalTo(1));

    response.then().body("successful[0]", Matchers.equalTo(true))
        .body("successful[1]", Matchers.equalTo(true))
        .body("successful[2]", Matchers.equalTo(true))
        .body("successful[3]", Matchers.equalTo(true))
        .body("successful[4]", Matchers.equalTo(true));*/
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    smartServer.cleanUp();
    cluster.cleanUp();
  }
}
