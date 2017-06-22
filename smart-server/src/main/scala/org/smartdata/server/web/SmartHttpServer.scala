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

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.apache.hadoop.conf.Configuration
import org.smartdata.conf.SmartConfKeys
import org.smartdata.server.SmartEngine

import scala.concurrent.Await
import scala.concurrent.duration._

class SmartHttpServer(ssmServer: SmartEngine, conf: Configuration) {
  implicit val system = ActorSystem("my-system")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val httpServerAddress: InetSocketAddress = {
    val strings = conf.get(SmartConfKeys.DFS_SSM_HTTP_ADDRESS_KEY,
      SmartConfKeys.DFS_SSM_HTTP_ADDRESS_DEFAULT).split(":")
    new InetSocketAddress(strings(strings.length - 2), strings(strings.length - 1).toInt)
  }

  def start(): Unit = {
    val route = new RestServices(ssmServer).route
    val bindingFuture = Http().bindAndHandle(route,
        httpServerAddress.getHostString, httpServerAddress.getPort)
    println(s"Please browse to http://${httpServerAddress.getHostString}:" +
      s"${httpServerAddress.getPort} to see the web UI")
    Await.result(bindingFuture, 15.seconds)
  }

  def stop(): Unit = {
    system.shutdown()
    system.awaitTermination()
  }

  def join(): Unit = {
  }
}

object SmartHttpServer extends App {
  val server = new SmartHttpServer(null, new Configuration())
  server.start()
}
