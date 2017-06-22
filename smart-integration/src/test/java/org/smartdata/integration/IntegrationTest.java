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
import io.restassured.matcher.RestAssuredMatchers;
import io.restassured.response.Response;
import io.restassured.response.Validatable;
import io.restassured.response.ValidatableResponse;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.integration.cluster.MiniSmartCluster;
import org.smartdata.integration.cluster.SmartCluster;

import java.net.URI;

/**
 * Integration test.
 */
public class IntegrationTest {
  private static SmartCluster cluster;
  private static SmartConf conf;
  private static IntegrationSmartServer smartServer;
  private static String httpUri;

  @BeforeClass
  public static void setup() throws Exception {
    // Set up an HDFS cluster
    cluster = new MiniSmartCluster();
    cluster.setUp();

    // Start a Smart server
    conf = cluster.getConf();
    httpUri = "localhost:7045";
    conf.set(SmartConfKeys.DFS_SSM_HTTP_ADDRESS_KEY, httpUri);
    smartServer = new IntegrationSmartServer();
    smartServer.setUp(conf);

    // Initialize url for RestApi
    //RestAssured.baseURI = "127.0.0.0";
    RestAssured.port = 7045;
  }

  @Test
  public void testRestApi() throws Exception {
    Response response = RestAssured.get("/api/v1.0/addrule/norule");
    ValidatableResponse validatable = response.then();
    while(true) {

    }
    //response.then().content();
    //response.then().body("title", Matchers.equalTo("rulelist"));
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    smartServer.cleanUp();
    cluster.cleanUp();
  }
}
