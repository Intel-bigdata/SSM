package org.smartdata.integration;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Created by root on 6/29/17.
 */
public class TestSystemRestApi extends IntegrationTestBase {
  // Just an example
  @Test
  public void testSubmitAction() throws Exception {
    Response response1 = RestAssured.get("/smart/api/v1/system/version");
    String json1 = response1.asString();
    response1.then().body("body", Matchers.equalTo("0.1.0"));

    Response response2 = RestAssured.get("/smart/api/v1/actions/registry/list");
    String json2 = response2.asString();
    ValidatableResponse validatableResponse = response2.then().root("body");
    validatableResponse.body("find { it.actionName == 'fsck' }.displayName", Matchers.equalTo("fsck"));
    validatableResponse.body("actionName", Matchers.hasItems("fsck",
        "diskbalance", "uncache", "setstoragepolicy", "blockec", "copy",
        "write", "stripec", "cache", "read", "allssd", "checkstorage",
        "archive", "list", "clusterbalance", "onessd", "hello"));

    /*
    Response response0 = RestAssured.get("/api/v1.0/actionlist");
    String json0 = response0.asString();
    // RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello -length 10");
    //Thread.sleep(2000);
    // RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello2 -length 10");
    //Thread.sleep(2000);
    // RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello3 -length 10");
    //Thread.sleep(2000);
    // RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello4 -length 10");
    //Thread.sleep(2000);
    // RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello5 -length 10");

    for (int i = 0; i < 10; i++) {
      RestAssured.post("/api/v1.0/submitaction/write?args=-file /hello"+
          + i + " -length 10");
      // Thread.sleep(2000);
    }

    Thread.sleep(5000);
    Response response = RestAssured.get("/api/v1.0/actionlist");
    String json = response.asString();
    List<ActionInfo> actionInfos = new Gson().fromJson(json, new TypeToken<List<ActionInfo>>(){}.getType());
    System.out.print(json);
    */

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
}
