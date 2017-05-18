package org.apache.hadoop.ssm.web

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.ssm.{SSMConfigureKeys, SSMServer}

import scala.concurrent.Await
import scala.concurrent.duration._

class SSMHttpServer(ssmServer: SSMServer, conf: Configuration) {
  implicit val system = ActorSystem("my-system")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val httpServerAddress: InetSocketAddress = {
    val strings = conf.get(SSMConfigureKeys.DFS_SSM_HTTP_ADDRESS_KEY,
        SSMConfigureKeys.DFS_SSM_HTTP_ADDRESS_DEFAULT).split(":")
    new InetSocketAddress(strings(strings.length - 2), strings(strings.length - 1).toInt)
  }

  def start(): Unit = {
    val route = new RestService().route
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

object SSMHttpServer extends App {
  val server = new SSMHttpServer(null, new Configuration())
  server.start()
}
