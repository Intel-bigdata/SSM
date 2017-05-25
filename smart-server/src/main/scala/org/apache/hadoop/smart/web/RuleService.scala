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
package org.apache.hadoop.smart.web

import java.util
import java.util.{HashMap, Map}

import scala.collection.JavaConverters._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directives.{complete, path}
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.google.gson.Gson
import org.apache.hadoop.smart.actions.ActionType
import org.apache.hadoop.smart.{CommandState, SmartServer}
import org.apache.hadoop.smart.rule.{RuleInfo, RuleState}
import org.apache.hadoop.smart.sql.CommandInfo
import org.apache.hadoop.smart.utils.JsonUtil

import scala.util.Random

class RuleService(ssmServer: SmartServer) extends BasicService {
  private val gson: Gson = new Gson()
  private val rules: util.Collection[RuleInfo] = new util.ArrayList[RuleInfo]()

  override protected def doRoute(implicit mat: Materializer): Route = pathPrefix("rules" / IntNumber) { ruleId =>
    path("start") {
      post {
        rules.asScala.filter(_.getId == ruleId).foreach(_.setState(RuleState.ACTIVE))
        complete("success")
      }
    } ~
    path("stop") {
      delete {
        rules.asScala.filter(_.getId == ruleId).foreach(_.setState(RuleState.DISABLED))
        complete("success")
      }
    } ~
    path("detail") {
      complete(gson.toJson(rules.asScala.find(_.getId == ruleId).get))
    } ~
    path("errors") {
      complete("{\"time\" : \"0\", \"error\" : \"\"}")
    } ~
    path("commands") {
      val smap1 = new util.HashMap[String, String]
      smap1.put("_FILE_PATH_", "/testCacheFile")
      val command1 = new CommandInfo(0, 1, ActionType.MoveFile,
        CommandState.PENDING, JsonUtil.toJsonString(smap1), 123123333l, 232444444l)
      val command2 = new CommandInfo(1, 1, ActionType.CacheFile, CommandState.PENDING,
        JsonUtil.toJsonString(smap1), 123178333l, 232444994l)
      complete(gson.toJson(util.Arrays.asList(command1, command2)))
    }
  } ~
    path("rulelist") {
      complete(gson.toJson(rules))
  } ~
  path("addrule") {
    post {
      entity(as[String]) { request =>
        val rule = java.net.URLDecoder.decode(request, "UTF-8")
        System.out.println("Adding rule: " + rule)
        addRuleInfo(rule)
        complete("Success")
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
