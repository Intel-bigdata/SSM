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

import java.util.List;

public class CmdletRestApi extends RestApiBase {

  /**
   * Submit an cmdlet.
   * @param cmdString
   * @return cmdlet id if success
   */
  public static Long submitCmdlet(String cmdString) {
    Response action = RestAssured.with().body(cmdString).post(CMDLETROOT + "/submit");
    //Response action = RestAssured.withArgs(cmdString).post(CMDLETROOT + "/submit");
    action.then().body("status", Matchers.equalTo("CREATED"));
    return new JsonPath(action.asString()).getLong("body");
  }

  public static boolean waitCmdletComplete(long cmdletId) {
    return waitCmdletComplete(cmdletId, Integer.MAX_VALUE);
  }

  /**
   * Wait until the specified cmdlet finishes.
   * @param cmdletId
   * @param secToWait
   * @return
   */
  public static boolean waitCmdletComplete(long cmdletId, int secToWait) {
    do {
      JsonPath path = getCmdletInfo(cmdletId);
      if (path.get("state").equals("DONE")) {
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

  public static JsonPath getCmdletInfo(long cmdletId) {
    Response cmdletInfo = RestAssured.get(CMDLETROOT + "/" + cmdletId + "/info");
    cmdletInfo.then().body("status", Matchers.equalTo("OK"));
    JsonPath path = cmdletInfo.jsonPath().setRoot("body");
    return path;
  }

  public static List<Long> getCmdletActionIds(long cmdletId) {
    return getCmdletInfo(cmdletId).getList("aids", Long.class);
  }
}
