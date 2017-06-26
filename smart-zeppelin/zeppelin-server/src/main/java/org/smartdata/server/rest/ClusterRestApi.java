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
import org.smartdata.metastore.tables.AccessCountTable;
import org.smartdata.metastore.utils.Constants;
import org.smartdata.server.SmartEngine;
import org.smartdata.server.rest.message.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Cluster APIs.
 */
@Path("/smart/api/v1/cluster")
@Produces("application/json")
public class ClusterRestApi {
  SmartEngine ssm;
  private static final Logger logger =
      LoggerFactory.getLogger(ClusterRestApi.class);
  Gson gson = new Gson();

  public ClusterRestApi(SmartEngine ssm) {
    this.ssm = ssm;
  }

  @GET
  @Path("/primary")
  public void primary() {
  }

  @GET
  @Path("/primary/cachedfiles")
  public Response cachedFiles() {
    return new JsonResponse<>(Response.Status.OK,
        ssm.getStatesManager().getCachedFileStatus()).build();
  }

  @GET
  @Path("/primary/hotfiles")
  public Response hotFiles() {
    List<AccessCountTable> tables =
        ssm.getStatesManager().getTablesInLast(Constants.ONE_HOUR_IN_MILLIS);
    return new JsonResponse<>(Response.Status.OK,
        ssm.getStatesManager().getHotFiles(tables, 20)).build();
  }

  @GET
  @Path("/alluxio/{clusterName}")
  public void alluxio() {
  }

  @GET
  @Path("/hdfs/{clusterName}")
  public void hdfs() {
  }

}
