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

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directives.{get, getFromResource, getFromResourceDirectory, path, pathEndOrSingleSlash, pathPrefix}
import akka.http.scaladsl.server.PathMatchers
import akka.stream.Materializer

class StaticRestService extends BasicService {
  override def cache: Boolean = true

  protected override def prefix = Neutral

  protected override def doRoute(implicit mat: Materializer) = {
    pathEndOrSingleSlash {
      getFromResource("dashboard/index.html")
    } ~
    pathPrefix("webjars") {
      get {
        getFromResourceDirectory("META-INF/resources/webjars")
      }
    } ~
    path(PathMatchers.Rest) { path =>
      getFromResource("dashboard/" + path)
    }
  }
}
