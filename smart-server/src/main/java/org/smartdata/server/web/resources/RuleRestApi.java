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
package org.smartdata.server.web.resources;

import com.google.gson.Gson;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.CmdletState;
import org.smartdata.common.cmdlet.CmdletInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.SmartServer;
import org.smartdata.server.utils.JsonUtil;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@Path("/api/v1.0")
@Produces("application/json")
public class RuleRestApi {
  private SmartServer ssm;
  private static final Logger logger =
      LoggerFactory.getLogger(RuleRestApi.class);
  Gson gson = new Gson();

  public RuleRestApi(SmartServer ssm) {
    this.ssm = ssm;
  }

  @GET
  public String getString() {
      return "hello";
  }

  @POST
  @Path("/rules/{ruleId}/start")
  public Response start(@PathParam("ruleId") String ruleId) throws Exception {
    logger.info("Start rule{}", ruleId);
    Long intNumer = Long.parseLong(ruleId);
    ssm.getRuleManager().activateRule(intNumer);
    return new JsonResponse<>(Response.Status.OK).build();
  }

  @DELETE
  @Path("/rules/{ruleId}/stop")
  public Response stop(@PathParam("ruleId") String ruleId) throws Exception {
    logger.info("Stop rule{}", ruleId);
    Long intNumer = Long.parseLong(ruleId);
    ssm.getRuleManager().disableRule(intNumer, true);
    return new JsonResponse<>(Response.Status.OK).build();
  }

  @GET
  @Path("/rules/{ruleId}/detail")
  public Response detail(@PathParam("ruleId") String ruleId) throws Exception {
    Long intNumer = Long.parseLong(ruleId);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getRuleManager().getRuleInfo(intNumer)).build();
  }

  @GET
  @Path("/rules/{ruleId}/errors")
  public Response errors(@PathParam("ruleId") String ruleId) {
    return new JsonResponse<>(Response.Status.NOT_FOUND).build();
  }

  @GET
  @Path("/rules/{ruleId}/cmdlets")
  public Response cmdlets(@PathParam("ruleId") String ruleId) throws Exception {
    Long intNumer = Long.parseLong(ruleId);
    Map<String, String> m = new HashMap<String, String>();
    m.put("_FILE_PATH_", "/testCacheFile");
    CmdletInfo cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.PENDING, JsonUtil.toJsonString(m), 123123333l, 232444444l);
    CmdletInfo cmdlet2 = new CmdletInfo(1, 1, CmdletState.PENDING,
        JsonUtil.toJsonString(m), 123178333l, 232444994l);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getCmdletExecutor().listCmdletsInfo(intNumer, null)).build();
  }

  @GET
  @Path("/rulelist")
  public Response ruleList() throws Exception {
    return new JsonResponse<>(Response.Status.OK, "",
        ssm.getRuleManager().listRulesInfo()).build();
  }

  @POST
  @Path("/addRule")
  public Response addRule(String message){
    String rule;
    long t;
    try {
      rule = java.net.URLDecoder.decode(message, "UTF-8");
      logger.info("Adding rule: " + rule);
      t = ssm.getRuleManager().submitRule(rule, RuleState.DISABLED);
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while adding rule ", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
    return new JsonResponse(Response.Status.CREATED, t).build();
  }
}
