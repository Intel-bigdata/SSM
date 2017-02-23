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

import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.ssm.web.resources.CommandParam;
import org.apache.hadoop.ssm.web.resources.GetOpParam;
import org.apache.hadoop.ssm.web.resources.PutOpParam;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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

  /** Handle HTTP GET request. */
  @GET
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
        String[] stdOutput = commandStatus.getOutput().getStdOutput();
        String[] stdError = commandStatus.getOutput().getStdError();
        final String js = JsonUtil.toJsonString("stdout", stdOutput)
          + JsonUtil.toJsonString("stderr", stdError);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
      default:
        throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  private Response get(GetOpParam op, UUID id) {
    switch (op.getValue()) {
      case GETCOMMANDSTATUS: {

      }
      default:
        throw new UnsupportedOperationException(op + " is not supported");
    }
  }

}
