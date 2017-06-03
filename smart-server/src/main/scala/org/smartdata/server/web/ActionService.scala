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

import scala.collection.JavaConverters._
import akka.http.scaladsl.server.Directives.{complete, path, pathPrefix, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet
import akka.stream.Materializer
import com.google.gson.Gson
import org.smartdata.common.actions.{ActionDescriptor, ActionInfo}
import org.smartdata.server.SmartServer

import scala.util.Random

class ActionService(ssmServer: SmartServer) extends BasicService {
  private val gson: Gson = new Gson()
  private val actions: util.Collection[ActionInfo] = new util.ArrayList[ActionInfo]()
  val builder = new ActionInfo.Builder()
    .setActionName("test")
    .setCreateTime(1024)
    .setFinished(true)
    .setFinishTime(2048)
    .setResult("this is result")
    .setLog("this is log")
    .setArgs(Array("abd", "def"))
      .setProgress(0.5f)
    .setResult("objc[41272]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/bin/java (0x1075fd4c0) and /Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/jre/lib/libinstrument.dylib (0x1076d94e0). One of the two will be used. Which one is undefined.")
    .setLog("objc[41272]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/bin/java (0x1075fd4c0) and /Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/jre/lib/libinstrument.dylib (0x1076d94e0). One of the two will be used. Which one is undefined.")
  actions.add(builder.build())

  private val actionTypes: util.Collection[ActionDescriptor] = new util.ArrayList[ActionDescriptor]()
  actionTypes.add(new ActionDescriptor("ls", "List files", "Comment", "Usage"))
  actionTypes.add(new ActionDescriptor("write", "Write files", "Comment", "Usage"))

  override protected def doRoute(implicit mat: Materializer): Route =
    pathPrefix("actions" / LongNumber) { actionId =>
      path("detail") {
        complete(gson.toJson(actions.asScala.find(_.getActionId == actionId).get))
      }
    } ~
      path("actiontypes") {
        complete(gson.toJson(actionTypes))
      } ~
      path("actionlist") {
        complete(gson.toJson(actions))
      } ~
      path("submitaction" / Segment) { actionType =>
        post {
          parameters(ParamMagnet("args")) { args: String =>
            val rule = java.net.URLDecoder.decode(args, "UTF-8")
            val action = new ActionInfo.Builder().setActionName(actionType)
              .setActionId(Math.abs(Random.nextInt()))
              .setArgs(args.split(" "))
              .setCreateTime(System.currentTimeMillis())
              .setFinished(false)
              .setSuccessful(false).build()
            actions.add(action)
            complete("Success")
          }
        }
      }
}
