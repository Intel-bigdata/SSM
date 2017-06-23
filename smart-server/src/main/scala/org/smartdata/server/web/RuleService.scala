/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.smartdata.server.web

import java.util

import akka.http.scaladsl.server.Directives.{complete, path, _}
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.google.gson.Gson
import org.smartdata.common.CmdletState
import org.smartdata.common.models.{CmdletInfo, RuleInfo}
import org.smartdata.common.rule.RuleState
import org.smartdata.server.SmartEngine
import org.smartdata.server.utils.JsonUtil

import scala.collection.JavaConverters._
import scala.util.Random

class RuleService(ssmServer: SmartEngine) extends BasicService {
  private val gson: Gson = new Gson()
  private val rules: util.Collection[RuleInfo] = new util.ArrayList[RuleInfo]()

  override protected def doRoute(implicit mat: Materializer): Route = pathPrefix("rules" / IntNumber) { ruleId =>
    path("start") {
      post {
        rules.asScala.filter(_.getId == ruleId).foreach(_.setState(RuleState.ACTIVE))
        try {
          ssmServer.getRuleManager.activateRule(ruleId)
          complete("success")
        } catch {
          case e: Exception => failWith(e)
        }
      }
    } ~
    path("stop") {
      delete {
        rules.asScala.filter(_.getId == ruleId).foreach(_.setState(RuleState.DISABLED))
        try {
          ssmServer.getRuleManager.disableRule(ruleId, true)
          complete("success")
        } catch {
          case e: Exception => failWith(e)
        }
      }
    } ~
    path("detail") {
      try {
        complete(gson.toJson(ssmServer.getRuleManager.getRuleInfo(ruleId)))
      } catch {
        case e: Exception => failWith(e)
      }
    } ~
    path("errors") {
      complete("{\"time\" : \"0\", \"error\" : \"\"}")
    } ~
    path("cmdlets") {
      val smap1 = new util.HashMap[String, String]
      smap1.put("_FILE_PATH_", "/testCacheFile")
      val cmdlet1 = new CmdletInfo(0, 1,
        CmdletState.PENDING, JsonUtil.toJsonString(smap1), 123123333l, 232444444l)
      val cmdlet2 = new CmdletInfo(1, 1, CmdletState.PENDING,
        JsonUtil.toJsonString(smap1), 123178333l, 232444994l)
      try {
        complete(gson.toJson(ssmServer.getCmdletExecutor.listCmdletsInfo(ruleId, null)))
      } catch {
        case e: Exception => failWith(e)
      }
    }
  } ~
    path("rulelist") {
      try {
        complete(gson.toJson(ssmServer.getRuleManager.listRulesInfo()))
      } catch {
        case e: Exception => failWith(e)
      }
  } ~
  path("addrule") {
    post {
      entity(as[String]) { request =>
        val rule = java.net.URLDecoder.decode(request, "UTF-8")
        System.out.println("Adding rule: " + rule)
        //addRuleInfo(rule)
        try {
          ssmServer.getRuleManager.submitRule(rule, RuleState.DISABLED)
          complete("Success")
        } catch {
          case e: Exception => failWith(e)
        }
      }
    }
  }

  private def addRuleInfo(rule: String): RuleInfo = {
    val builder = RuleInfo.newBuilder
    builder.setRuleText(rule).setId(Math.abs(Random.nextInt())).setState(RuleState.DISABLED)
    val ruleInfo = builder.build
    rules.add(ruleInfo)
    ruleInfo
  }
}
