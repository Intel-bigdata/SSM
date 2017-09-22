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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ClusterRestApi extends RuleRestApi {

  public static JsonPath getCachedFiles() {
    Response resp = RestAssured.get(PRIMCLUSTERROOT + "/cachedfiles");
    resp.then().body("status", equalTo("OK"));
    return resp.jsonPath().setRoot("body");
  }

  public static JsonPath getFileInfo(String path) {
    Response resp = RestAssured.with().body(path).get(PRIMCLUSTERROOT + "/fileinfo");
    resp.then().body("status", equalTo("OK"));
    return resp.jsonPath().setRoot("body");
  }

  /**
   * Get list of file paths that been cached.
   * @return
   */
  public static List<String> getCachedFilePaths() {
    return getCachedFiles().getList("path", String.class);
  }
}
