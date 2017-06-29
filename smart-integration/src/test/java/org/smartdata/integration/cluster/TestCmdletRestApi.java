package org.smartdata.integration.cluster;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.junit.Test;
import org.smartdata.integration.IntegrationTestBase;


/**
 * Created by root on 6/29/17.
 */
public class TestCmdletRestApi extends IntegrationTestBase {
  @Test
  public void test() throws Exception {
    Response response0 = RestAssured.post("/smart/api/v1/cmdlets/submit/write?args=hello");
    Response response = RestAssured.post("/smart/api/v1/rules/add?args=file : accessCount(10s) > 5 | cache");
    Thread.sleep(1000);
  }
}
