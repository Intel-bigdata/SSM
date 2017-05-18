package org.apache.hadoop.ssm.web

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Directives.{get, getFromResource, getFromResourceDirectory, path, pathEndOrSingleSlash, pathPrefix}
import akka.http.scaladsl.server.PathMatchers

class RestService {
  val route =
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
