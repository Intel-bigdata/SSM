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
@Path("/cmdlets")
@Produces("application/json")
public class CmdletRestApi {
  private SmartEngine smartEngine;
  private static final Logger logger =
          LoggerFactory.getLogger(CmdletRestApi.class);

  public CmdletRestApi(SmartEngine smartEngine) {
    this.smartEngine = smartEngine;
  }

  @GET
  @Path("/{cmdletId}/info")
  public Response info(@PathParam("cmdletId") String cmdletId) {
    Long longNumber = Long.parseLong(cmdletId);
    try {
      return new JsonResponse<>(Response.Status.OK,
              smartEngine.getCmdletManager().getCmdletInfo(longNumber)).build();
    } catch (Exception e) {
      logger.error("Exception in CmdletRestApi while getting info", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
              e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/list")
  public Response list() {
    try {
      return new JsonResponse<>(Response.Status.OK,
              smartEngine.getCmdletManager()
                      .listCmdletsInfo(-1, null))
              .build();
    } catch (Exception e) {
      logger.error("Exception in CmdletRestApi while listing cmdlets", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
              e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @POST
  @Path("/submit")
  public Response submitCmdlet(String args) {
    try {
      return new JsonResponse<>(Response.Status.CREATED, smartEngine.getCmdletManager()
              .submitCmdlet(args)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while adding cmdlet", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
              e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @POST
  @Path("/{cmdletId}/stop")
  public Response stop(@PathParam("cmdletId") String cmdletId) {
    Long longNumber = Long.parseLong(cmdletId);
    try {
      smartEngine.getCmdletManager().disableCmdlet(longNumber);
      return new JsonResponse<>(Response.Status.OK).build();
    } catch (Exception e) {
      logger.error("Exception in CmdletRestApi while stop cmdlet " + longNumber, e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
              e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
