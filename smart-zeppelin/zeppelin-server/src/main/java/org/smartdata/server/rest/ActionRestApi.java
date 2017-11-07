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

import com.amazonaws.util.json.JSONArray;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.ActionRegistry;
import org.smartdata.server.SmartEngine;
import org.smartdata.server.rest.message.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Action APIs.
 */
@Path("/actions")
@Produces("application/json")
public class ActionRestApi {
  SmartEngine smartEngine;
  private static final Logger logger =
      LoggerFactory.getLogger(ActionRestApi.class);

  public ActionRestApi(SmartEngine smartEngine) {
    this.smartEngine = smartEngine;
  }

  @GET
  @Path("/registry/list")
  public Response actionTypes() {
    try {
      return new JsonResponse<>(Response.Status.OK,
          ActionRegistry.supportedActions()).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while listing action types", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/list/{pageIndex}/{numPerPage}/{orderBy}/{isDesc}")
  public Response actionList(@PathParam("pageIndex") String pageIndex,
      @PathParam("numPerPage") String numPerPage,
      @PathParam("orderBy") String orderBy,
      @PathParam("isDesc") String isDesc) {
    if (logger.isDebugEnabled()) {
      logger.debug("pageIndex={}, numPerPage={}, orderBy={}, isDesc={}",
          pageIndex, numPerPage, orderBy, isDesc);
    }
    try {
      List<String> orderByList = Arrays.asList(orderBy.split(","));
      List<String> isDescStringList = Arrays.asList(isDesc.split(","));
      List<Boolean> isDescList = new ArrayList<>();
      for (int i = 0; i < isDescStringList.size(); i++) {
        isDescList.add(Boolean.parseBoolean(isDescStringList.get(i)));
      }
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager().listActions(Long.parseLong(pageIndex),
              Long.parseLong(numPerPage), orderByList, isDescList)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while listing action types", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/list/{listNumber}/{ruleId}")
  public Response actionList(@PathParam("listNumber") String listNumber,
      @PathParam("ruleId") String ruleId) {
    Integer intNumber = Integer.parseInt(listNumber);
    intNumber = intNumber > 0 ? intNumber : 0;
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager().getActions(Long.valueOf(ruleId), intNumber)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while listing action types", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/filelist/{ruleId}/{pageIndex}/{numPerPage}")
  public Response dataSyncFileList(@PathParam("ruleId") String ruleId,
      @PathParam("pageIndex") String pageIndex,
      @PathParam("numPerPage") String numPerPage) {
    if (logger.isDebugEnabled()) {
      logger.debug("ruleId={}, pageIndex={}, numPerPage={}", ruleId,
          pageIndex, numPerPage);
    }
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager().getFileActions(Long.valueOf(ruleId),
              Long.valueOf(pageIndex), Long.valueOf(numPerPage))).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while listing file actions", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/filelist/{listNumber}/{ruleId}")
  public Response actionFileList(@PathParam("listNumber") String listNumber,
      @PathParam("ruleId") String ruleId) {
    Integer intNumber = Integer.parseInt(listNumber);
    intNumber = intNumber > 0 ? intNumber : 0;
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager().getFileActions(Long.valueOf(ruleId), intNumber)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while listing file actions", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }


  @GET
  @Path("/type/{listNumber}/{actionName}")
  public Response actionTypeList(@PathParam("listNumber") String listNumber,
      @PathParam("actionName") String actionName) {
    Integer intNumber = Integer.parseInt(listNumber);
    intNumber = intNumber > 0 ? intNumber : 0;
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager()
              .listNewCreatedActions(actionName, intNumber)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while listing action types", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/{actionId}/info")
  public Response info(@PathParam("actionId") String actionId) {
    Long longNumber = Long.parseLong(actionId);
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getCmdletManager().getActionInfo(longNumber)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while getting info", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
