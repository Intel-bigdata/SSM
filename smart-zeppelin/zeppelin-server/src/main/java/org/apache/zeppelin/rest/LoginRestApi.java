/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.rest;

import com.google.common.hash.Hashing;
import org.apache.shiro.authc.*;
import org.apache.shiro.subject.Subject;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.server.SmartZeppelinServer;
import org.apache.zeppelin.ticket.TicketContainer;
import org.apache.zeppelin.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.model.UserInfo;
import org.smartdata.server.SmartEngine;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created for org.apache.zeppelin.rest.message on 17/03/16.
 */

@Path("/login")
@Produces("application/json")
public class LoginRestApi {
  private SmartEngine engine = new SmartZeppelinServer().getEngine();
  private static final Logger LOG = LoggerFactory.getLogger(LoginRestApi.class);

  /**
   * Required by Swagger.
   */
  public LoginRestApi() {
    super();
  }


  /**
   * Post Login
   * Returns userName & password
   * for anonymous access, username is always anonymous.
   * After getting this ticket, access through websockets become safe
   *
   * @return 200 response
   */
  @POST
  @ZeppelinApi
  public Response postLogin(@FormParam("userName") String userName,
                            @FormParam("password") String password) {
    JsonResponse response = null;
    // ticket set to anonymous for anonymous user. Simplify testing.
    Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    if (currentUser.isAuthenticated()) {
      currentUser.logout();
    }
    boolean flag = false;
    try {
      password = Hashing.sha256().hashString(password, StandardCharsets.UTF_8).toString();
      flag = engine.getCmdletManager().authentic(new UserInfo(userName, password));
    } catch (Exception e) {
      LOG.error("Exception in login: ", e);
    }
    if (!currentUser.isAuthenticated() && flag) {
      userName = "admin";
      password = "ssm@123";
      try {
        UsernamePasswordToken token = new UsernamePasswordToken(userName, password);
        //      token.setRememberMe(true);

        currentUser.getSession().stop();
        currentUser.getSession(true);
        currentUser.login(token);

        HashSet<String> roles = SecurityUtils.getRoles();
        String principal = SecurityUtils.getPrincipal();
        String ticket;
        if ("anonymous".equals(principal))
          ticket = "anonymous";
        else
          ticket = TicketContainer.instance.getTicket(principal);

        Map<String, String> data = new HashMap<>();
        data.put("principal", principal);
        data.put("roles", roles.toString());
        data.put("ticket", ticket);

        response = new JsonResponse(Response.Status.OK, "", data);
        //if no exception, that's it, we're done!

        //set roles for user in NotebookAuthorization module
        NotebookAuthorization.getInstance().setRoles(principal, roles);
      } catch (UnknownAccountException uae) {
        //username wasn't in the system, show them an error message?
        LOG.error("Exception in login: ", uae);
      } catch (IncorrectCredentialsException ice) {
        //password didn't match, try again?
        LOG.error("Exception in login: ", ice);
      } catch (LockedAccountException lae) {
        //account for that username is locked - can't login.  Show them a message?
        LOG.error("Exception in login: ", lae);
      } catch (AuthenticationException ae) {
        //unexpected condition - error?
        LOG.error("Exception in login: ", ae);
      }
    }

    if (response == null) {
      response = new JsonResponse(Response.Status.FORBIDDEN, "", "");
    }

    LOG.warn(response.toString());
    return response.build();
  }

  @POST
  @Path("logout")
  @ZeppelinApi
  public Response logout() {
    JsonResponse response;
    Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    currentUser.logout();
    response = new JsonResponse(Response.Status.UNAUTHORIZED, "", "");
    LOG.warn(response.toString());
    return response.build();
  }

  @POST
  @Path("newPassword")
  @ZeppelinApi
  public Response postPassword(@FormParam("userName") String userName,
                               @FormParam("oldPassword") String oldPassword,
                               @FormParam("newPassword1") String newPassword,
                               @FormParam("newPassword2") String newPassword2) {
    LOG.info("UserName: " + userName + " OldPass: " + oldPassword + " newPass1: "
        + newPassword + " newPass2: " + newPassword2);
    JsonResponse response = null;
    // ticket set to anonymous for anonymous user. Simplify testing.
    Subject currentUser = org.apache.shiro.SecurityUtils.getSubject();
    if (currentUser.isAuthenticated()) {
      currentUser.logout();
    }
    boolean flag = false;
    try {
      String password = Hashing.sha256().hashString(oldPassword, StandardCharsets.UTF_8).toString();
      flag = engine.getCmdletManager().authentic(new UserInfo(userName, password));
    } catch (Exception e) {
      LOG.error("Exception in login: ", e);
    }
    if (flag) {
      LOG.info("in changing pass");
      if (newPassword.equals(newPassword2)) {
        try {
          engine.getCmdletManager().newPassword(new UserInfo(userName, newPassword));
          LOG.info("changing pass");
        } catch (Exception e) {
          LOG.error("Exception in setting password: ", e);
        }
      }
    }
    if (!currentUser.isAuthenticated() && flag) {
      userName = "admin";
      String password = "ssm@123";
      try {
        UsernamePasswordToken token = new UsernamePasswordToken(userName, password);
        //      token.setRememberMe(true);

        currentUser.getSession().stop();
        currentUser.getSession(true);
        currentUser.login(token);

        HashSet<String> roles = SecurityUtils.getRoles();
        String principal = SecurityUtils.getPrincipal();
        String ticket;
        if ("anonymous".equals(principal))
          ticket = "anonymous";
        else
          ticket = TicketContainer.instance.getTicket(principal);

        Map<String, String> data = new HashMap<>();
        data.put("principal", principal);
        data.put("roles", roles.toString());
        data.put("ticket", ticket);

        response = new JsonResponse(Response.Status.OK, "", data);
        //if no exception, that's it, we're done!

        //set roles for user in NotebookAuthorization module
        NotebookAuthorization.getInstance().setRoles(principal, roles);
      } catch (UnknownAccountException uae) {
        //username wasn't in the system, show them an error message?
        LOG.error("Exception in login: ", uae);
      } catch (IncorrectCredentialsException ice) {
        //password didn't match, try again?
        LOG.error("Exception in login: ", ice);
      } catch (LockedAccountException lae) {
        //account for that username is locked - can't login.  Show them a message?
        LOG.error("Exception in login: ", lae);
      } catch (AuthenticationException ae) {
        //unexpected condition - error?
        LOG.error("Exception in login: ", ae);
      }
    }

    if (response == null) {
      response = new JsonResponse(Response.Status.FORBIDDEN, "", "");
    }

    LOG.warn(response.toString());
    return response.build();
  }
}
