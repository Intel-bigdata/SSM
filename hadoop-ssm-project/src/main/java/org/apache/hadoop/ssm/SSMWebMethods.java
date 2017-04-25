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
package org.apache.hadoop.ssm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.ssm.web.resources.CommandParam;
import org.apache.hadoop.ssm.web.resources.GetOpParam;
import org.apache.hadoop.ssm.web.resources.PutOpParam;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * SSM web methods implementation.
 */
@Path("")
@ResourceFilters(ParamFilter.class)
public class SSMWebMethods {
  public static final Log LOG = LogFactory.getLog(SSMWebMethods.class);

  private @Context ServletContext context;
  private @Context HttpServletResponse response;

  /** Handle HTTP PUT request. */
  @PUT
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response put(
    @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
    @QueryParam(CommandParam.NAME) @DefaultValue(CommandParam.DEFAULT)
          final CommandParam cmd
  ) {
    return put(op, cmd.getValue());
  }

  private Response put(PutOpParam op, String cmd) {
    switch (op.getValue()) {
      case ADDRULE: {

      }
      case RUNCOMMAND: {
        CommandPool commandPool = CommandPool.getInstance();
        UUID commandId = commandPool.runCommand(cmd);
        while (!commandPool.getCommandStatus(commandId).isFinished()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        CommandStatus commandStatus = commandPool.getCommandStatus(commandId);
        ObjectMapper MAPPER = new ObjectMapper();
        String js = null;
        try {
          js = MAPPER.writeValueAsString(toJsonMap(commandStatus.getOutput()));
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
      default:
        throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  public static Map<String, Object> toJsonMap(final CommandStatus.OutPutType outPut) {
    if (outPut == null) {
      return null;
    }
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("stdout", outPut.getStdOutput());
    m.put("stderr", outPut.getStdError());
    return m;
  }

  /** Handle HTTP GET request. */
  @GET
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
    MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response get(
    @QueryParam(GetOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
    final GetOpParam op,
    @QueryParam(CommandParam.NAME) @DefaultValue(CommandParam.DEFAULT)
    final CommandParam cmd

  ) throws IOException, InterruptedException {
    return get(op, cmd.getValue());
  }

  private Response get(GetOpParam op, String cmd) {
    switch (op.getValue()) {
      case SHOWCACHE: {
        final Map<String, Object> m = new TreeMap<String, Object>();
        m.put("cacheCapacity", 3);
        m.put("cacheUsed", 1);
        m.put("cacheRemaining", 2);
        m.put("cacheUsedPercentage", 33);
        ObjectMapper MAPPER = new ObjectMapper();
        String js = null;
        try {
          js = MAPPER.writeValueAsString(m);
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
      case GETCOMMANDSTATUS: {

      }
      default:
        throw new UnsupportedOperationException(op + " is not supported");
    }
  }

}
