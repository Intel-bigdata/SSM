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
import org.smartdata.common.actions.ActionInfo;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.conf.SmartConf;
import org.smartdata.server.SmartServer;
import org.smartdata.server.metastore.tables.AccessCountTable;
import org.smartdata.server.utils.Constants;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
@Path("/api/v1.0")
@Produces("application/json")
public class ActionRestApi {
  private static final Logger logger =
      LoggerFactory.getLogger(ActionRestApi.class);
  Gson gson = new Gson();
  private Collection<ActionInfo> actions = new ArrayList<ActionInfo>();

  public ActionRestApi() {
  }

  @GET
  @Path("/actions/{actionId}/detail")
  public Response detail(@PathParam("actionId") String actionId)
      throws Exception {
    SmartConf conf = new SmartConf();
    SmartServer ssm = SmartServer.createSSM(null, conf);
    Long longNumer = Long.parseLong(actionId);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getCmdletExecutor().getActionInfo(longNumer)).build();
  }

  @GET
  @Path("/cachedfiles")
  public Response cachedfiles() throws Exception {
    SmartConf conf = new SmartConf();
    SmartServer ssm = SmartServer.createSSM(null, conf);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getDBAdapter().getCachedFileStatus()).build();
  }

  @GET
  @Path("/hotfiles")
  public Response hotfiles() throws Exception {
    SmartConf conf = new SmartConf();
    SmartServer ssm = SmartServer.createSSM(null, conf);
    List<AccessCountTable> tables =
        ssm.getStatesManager().getTablesInLast(Constants.ONE_HOUR_IN_MILLIS);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getDBAdapter().getHotFiles(tables, 20)).build();
  }

  @GET
  @Path("/actiontypes")
  public Response actiontypes() throws Exception {
    SmartConf conf = new SmartConf();
    SmartServer ssm = SmartServer.createSSM(null, conf);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getCmdletExecutor().listActionsSupported()).build();
  }

  @POST
  @Path("/submitaction/{actionType}")
  public Response submitAction(String args,
      @PathParam("actionType") String actionType) {
    SmartConf conf = new SmartConf();
    SmartServer ssm;
    try {
      ssm = SmartServer.createSSM(null, conf);
      ActionInfo request = gson.fromJson(args, ActionInfo.class);
      ActionInfo action = new ActionInfo.Builder().setActionName(actionType)
          .setActionId(Math.abs(new Random().nextLong()))
          .setArgs(CmdletDescriptor.fromCmdletString(actionType + " " + args)
              .getActionArgs(0))
          .setCreateTime(System.currentTimeMillis())
          .setFinished(false)
          .setSuccessful(false).build();
      actions.add(action);
      logger.info("New repository {} added", request.getActionId());
      return new JsonResponse(Response.Status.CREATED, ssm.getCmdletExecutor()
          .submitCmdlet(actionType + " " + args)).build();
    } catch (Exception e) {
      logger.error("Exception in ActionRestApi while adding action ", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
