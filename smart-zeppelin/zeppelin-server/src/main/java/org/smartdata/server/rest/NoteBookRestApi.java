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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Notebook Api
 */
@Path("/note")
@Produces("application/json")
public class NoteBookRestApi {
  SmartEngine smartEngine;
  private static final Logger LOG = LoggerFactory.getLogger(NoteBookRestApi.class);

  public NoteBookRestApi(SmartEngine smartEngine) {
    this.smartEngine = smartEngine;
  }
  @GET
  @Path("/info")
  public Response getNotebookInfo() {
    LOG.info("Request to get notebook info");
    File notebookJson = new File("notebook/note.json");
    BufferedReader bufferedReader = null;
    StringBuilder stringBuilder = new StringBuilder();
    try {
      bufferedReader = new BufferedReader(new FileReader(notebookJson));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        stringBuilder.append(line);
      }
    } catch (Exception e) {
      LOG.error("Exception in NotebookRestApi while get notebook info", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR,
          e.getMessage(), ExceptionUtils.getStackTrace(e)).build();
    }
    return Response.status(Response.Status.OK).entity(stringBuilder.toString()).build();
  }
}
