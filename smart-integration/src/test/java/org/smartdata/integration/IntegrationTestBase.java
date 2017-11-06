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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.integration.cluster.SmartCluster;
import org.smartdata.integration.cluster.SmartMiniCluster;

/**
 * Integration test base.
 */
public class IntegrationTestBase {

  protected static SmartCluster cluster;
  protected static SmartConf conf;
  protected static IntegrationSmartServer smartServer;
  private static int zeppelinPort;

  @BeforeClass
  public static void setup() throws Exception {
    // Set up an HDFS cluster
    conf = new SmartConf();
    String nn = conf.get(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY);
    if (nn == null || nn.length() == 0) {
      System.out.println("Setting up an mini cluster for testing");
      cluster = new SmartMiniCluster();
      cluster.setUp();
      conf = cluster.getConf();
    } else {
      System.out.println("Using extern HDFS cluster:" + nn);
    }

    // Start a Smart server
    String httpAddr = conf.get(SmartConfKeys.SMART_SERVER_HTTP_ADDRESS_KEY,
        SmartConfKeys.SMART_SERVER_HTTP_ADDRESS_DEFAULT);
    zeppelinPort = Integer.parseInt(httpAddr.split(":")[1]);
    conf.setBoolean(SmartConfKeys.SMART_ENABLE_ZEPPELIN_WEB, false);
    smartServer = new IntegrationSmartServer();
    smartServer.setUp(conf);

    // Initialize RestAssured
    initRestAssured();
  }

  private static void initRestAssured() {
    RestAssured.port = zeppelinPort;
    //RestAssured.registerParser("text/plain", Parser.JSON);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    if (smartServer != null) {
      smartServer.cleanUp();
    }
    if (cluster != null) {
      cluster.cleanUp();
    }
  }
}
