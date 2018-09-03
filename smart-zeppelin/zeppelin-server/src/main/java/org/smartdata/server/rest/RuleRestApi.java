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
package org.smartdata.server.rest;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.RuleState;
import org.smartdata.server.SmartEngine;
import org.smartdata.server.rest.message.JsonResponse;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Rules APIs.
 */
@Path("/rules")
@Produces("application/json")
public class RuleRestApi {
  private SmartEngine smartEngine;
  private static final Logger logger =
      LoggerFactory.getLogger(RuleRestApi.class);

  public RuleRestApi(SmartEngine smartEngine) {
    this.smartEngine = smartEngine;
  }

  @POST
  @Path("/add")
  public Response addRule(@FormParam("ruleText") String ruleText) {
    String rule;
    long t;
    try {
      logger.info("Adding rule: " + ruleText);
      t = smartEngine.getRuleManager().submitRule(ruleText, RuleState.DISABLED);
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while adding rule: ", e.getMessage());
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
    return new JsonResponse(Response.Status.CREATED, t).build();
  }

  @POST
  @Path("/{ruleId}/delete")
  public Response deleteRule(@PathParam("ruleId") String ruleId) {
    try {
      Long longNumber = Long.parseLong(ruleId);
      smartEngine.getRuleManager().deleteRule(longNumber, false);
      return new JsonResponse<>(Response.Status.OK).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while deleting rule ", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @POST
  @Path("/{ruleId}/start")
  public Response start(@PathParam("ruleId") String ruleId) {
    logger.info("Start rule{}", ruleId);
    Long intNumber = Long.parseLong(ruleId);
    try {
      smartEngine.getRuleManager().activateRule(intNumber);
      return new JsonResponse<>(Response.Status.OK).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while starting rule ", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @POST
  @Path("/{ruleId}/stop")
  public Response stop(@PathParam("ruleId") String ruleId) {
    logger.info("Stop rule{}", ruleId);
    Long intNumber = Long.parseLong(ruleId);
    try {
      smartEngine.getRuleManager().disableRule(intNumber, true);
      return new JsonResponse<>(Response.Status.OK).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while stopping rule ", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/{ruleId}/info")
  public Response info(@PathParam("ruleId") String ruleId) {
    Long intNumber = Long.parseLong(ruleId);
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getRuleManager().getRuleInfo(intNumber)).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while getting rule info", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/{ruleId}/cmdlets/{pageIndex}/{numPerPage}/{orderBy}/{isDesc}")
  public Response cmdlets(@PathParam("ruleId") String ruleId,
      @PathParam("pageIndex") String pageIndex,
      @PathParam("numPerPage") String numPerPage,
      @PathParam("orderBy") String orderBy,
      @PathParam("isDesc") String isDesc) {
    Long longNumber = Long.parseLong(ruleId);
    if (logger.isDebugEnabled()) {
      logger.debug("ruleId={}, pageIndex={}, numPerPage={}, orderBy={}, " +
              "isDesc={}", longNumber, pageIndex, numPerPage, orderBy, isDesc);
    }
    try {
      List<String> orderByList = Arrays.asList(orderBy.split(","));
      List<String> isDescStringList = Arrays.asList(isDesc.split(","));
      List<Boolean> isDescList = new ArrayList<>();
      for (int i = 0; i < isDescStringList.size(); i++) {
        isDescList.add(Boolean.parseBoolean(isDescStringList.get(i)));
      }
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager().listCmdletsInfo(longNumber,
              Integer.parseInt(pageIndex),
              Integer.parseInt(numPerPage), orderByList, isDescList)).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while getting cmdlets", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/{ruleId}/cmdlets")
  public Response cmdlets(@PathParam("ruleId") String ruleId) {
    Long intNumber = Long.parseLong(ruleId);
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager().listCmdletsInfo(intNumber, null)).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while getting cmdlets", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/list")
  public Response ruleList() {
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getRuleManager().listRulesInfo()).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while listing rules", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/list/move")
  public Response ruleMoveList() {
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getRuleManager().listRulesMoveInfo()).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while listing Move rules", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/list/sync")
  public Response ruleSyncList() {
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getRuleManager().listRulesSyncInfo()).build();
    } catch (Exception e) {
      logger.error("Exception in RuleRestApi while listing Sync rules", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
