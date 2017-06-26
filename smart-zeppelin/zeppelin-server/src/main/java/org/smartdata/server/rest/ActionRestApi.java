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

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.actions.ActionRegistry;
import org.smartdata.server.SmartEngine;
import org.smartdata.server.rest.message.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Action APIs.
 */
@Path("/smart/api/v1/actions")
@Produces("application/json")
public class ActionRestApi {
  SmartEngine ssm;
  private static final Logger logger =
      LoggerFactory.getLogger(ActionRestApi.class);
  Gson gson = new Gson();

  public ActionRestApi(SmartEngine ssm) {
    this.ssm = ssm;
  }

  @GET
  @Path("/registry/list")
  public Response actionTypes() throws Exception {
    return new JsonResponse<>(Response.Status.OK,
      ActionRegistry.supportedActions()).build();
  }

  @GET
  @Path("/list")
  public Response actionList() throws Exception {
    return new JsonResponse<>(Response.Status.OK,
        ssm.getCmdletExecutor().listNewCreatedActions(20)).build();
  }

  @GET
  @Path("/{actionId}/status")
  public void status() {
  }

  @GET
  @Path("/{actionId}/detail")
  public Response detail(@PathParam("actionId") String actionId) throws Exception {
    Long longNumer = Long.parseLong(actionId);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getCmdletExecutor().getActionInfo(longNumer)).build();
  }

  @GET
  @Path("/{actionId}/summary")
  public void summary() {
  }
}
