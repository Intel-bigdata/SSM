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
import org.smartdata.server.SmartEngine;
import org.smartdata.server.rest.message.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Cmdlets APIs.
 */
@Path("/smart/api/v1/cmdlets")
@Produces("application/json")
public class CmdletRestApi {
  private SmartEngine ssm;
  private static final Logger logger =
      LoggerFactory.getLogger(CmdletRestApi.class);

  public CmdletRestApi(SmartEngine ssm) {
    this.ssm = ssm;
  }

  @GET
  @Path("/{cmdletId}/status")
  public Response status(@PathParam("cmdletId") String cmdletId)
      throws Exception {
    Long longNumber = Long.parseLong(cmdletId);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getCmdletExecutor().getCmdletInfo(longNumber)).build();
  }

  @GET
  @Path("/list")
  public Response list() throws Exception {
    return new JsonResponse<>(Response.Status.OK,
        ssm.getCmdletExecutor().listCmdletsInfo(-1, null))
        .build();
  }

  @POST
  @Path("/submit/{actionType}")
  public Response submitAction(String args,
      @PathParam("actionType") String actionType) {
    try {
      return new JsonResponse<>(Response.Status.CREATED, ssm.getCmdletExecutor()
          .submitCmdlet(actionType + " " + args)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while adding action ", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
