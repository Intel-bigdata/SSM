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
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.dao.AccessCountTable;
import org.smartdata.metastore.utils.Constants;
import org.smartdata.server.SmartEngine;
import org.smartdata.server.rest.message.JsonResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;

/**
 * Cluster APIs.
 */
@Path("/cluster")
@Produces("application/json")
public class ClusterRestApi {
  SmartEngine smartEngine;
  private static final Logger logger =
      LoggerFactory.getLogger(ClusterRestApi.class);

  public ClusterRestApi(SmartEngine smartEngine) {
    this.smartEngine = smartEngine;
  }

  @GET
  @Path("/primary")
  public Response primary() {
    // return NN url
    try {
      String namenodeUrl = smartEngine.getConf().
          get(SmartConfKeys.SMART_DFS_NAMENODE_RPCSERVER_KEY);
      return new JsonResponse<>(Response.Status.OK,
          "Namenode URL", namenodeUrl).build();
    } catch (Exception e) {
      logger.error("Exception in ClusterRestApi while getting primary info", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/primary/cachedfiles")
  public Response cachedFiles() {
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getStatesManager().getCachedFileStatus()).build();
    } catch (Exception e) {
      logger.error("Exception in ClusterRestApi while listing cached files", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/primary/hotfiles")
  public Response hotFiles() {
    try {
      List<AccessCountTable> tables =
          smartEngine.getStatesManager().getTablesInLast(Constants.ONE_HOUR_IN_MILLIS);
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getStatesManager().getHotFiles(tables, 20)).build();
    } catch (Exception e) {
      logger.error("Exception in ClusterRestApi while listing hot files", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/primary/utilization/{resourceName}")
  public Response utilization(@PathParam("resourceName") String resourceName) {
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getUtilization(resourceName)).build();
    } catch (Exception e) {
      logger.error("Exception in ClusterRestApi while getting [" + resourceName
          + "] utilization", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   *
   * @param resourceName
   * @param timeGranularity  Time interval of successive data points in milliseconds
   * @param beginTs  Begin timestamp in milliseconds. If <=0 denotes the value related to 'endTs'
   * @param endTs  Like 'beginTs'. If <= 0 denotes the time related to current server time.
   * @return
   */
  @GET
  @Path("/primary/hist_utilization/{resourceName}/{timeGranularity}/{beginTs}/{endTs}")
  public Response utilization(@PathParam("resourceName") String resourceName,
      @PathParam("timeGranularity") String timeGranularity,
      @PathParam("beginTs") String beginTs,
      @PathParam("endTs") String endTs) {
    try {
      long now = System.currentTimeMillis();
      long granularity = Long.valueOf(timeGranularity);
      long tsEnd = Long.valueOf(endTs);
      long tsBegin = Long.valueOf(beginTs);
      if (granularity == 0) {
        granularity = 1;
      }
      if (tsEnd <= 0) {
        tsEnd += now;
      }
      if (tsBegin <= 0) {
        tsBegin += tsEnd;
      }
      if (tsBegin > tsEnd) {
        return new JsonResponse<>(Status.BAD_REQUEST, "Invalid time range").build();
      }

      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getHistUtilization(resourceName, granularity, tsBegin, tsEnd)).build();
    } catch (Exception e) {
      logger.error("Exception in ClusterRestApi while getting [" + resourceName
          + "] utilization", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GET
  @Path("/primary/fileinfo")
  public Response fileInfo(String path) {
    try {
      return new JsonResponse<>(Response.Status.OK,
          smartEngine.getStatesManager().getFileInfo(path)).build();
    } catch (Exception e) {
      logger.error("Exception in ClusterRestApi while listing hot files", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }

//  @GET
//  @Path("/alluxio/{clusterName}")
//  public void alluxio() {
//  }
//
//  @GET
//  @Path("/hdfs/{clusterName}")
//  public void hdfs() {
//  }
}
