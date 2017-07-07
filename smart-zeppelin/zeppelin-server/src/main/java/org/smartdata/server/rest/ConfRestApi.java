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
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Conf APIs.
 */
@Path("/conf")
@Produces("application/json")
public class ConfRestApi {
  SmartEngine smartEngine;
  private static final Logger logger =
      LoggerFactory.getLogger(ConfRestApi.class);

  public ConfRestApi(SmartEngine smartEngine) {
    this.smartEngine = smartEngine;
  }

  @GET
  @Path("")
  public Response conf() {
    try {
      Iterator<Map.Entry<String, String>> conf = smartEngine.getConf().iterator();
      Map<String, String> confMap = new HashMap<>();
      while (conf.hasNext()) {
        Map.Entry<String, String> confEntry = conf.next();
        confMap.put(confEntry.getKey(), confEntry.getValue());
      }
      return new JsonResponse<>(Response.Status.OK, confMap).build();
    } catch (Exception e) {
      logger.error("Exception while getting configuration", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
        e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
