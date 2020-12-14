package com.fferrari

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import com.fferrari.actor.TaskManagerActor
import com.fferrari.actor.protocol.TaskManagerRequestProtocol
import com.fferrari.model.{ServiceDeployment, ServiceDeploymentJsonProtocol}

import scala.io.StdIn

object NexthinkChallengeApp
  extends App
    with ServiceDeploymentJsonProtocol
    with SprayJsonSupport {
  implicit val actorSystem = ActorSystem(TaskManagerActor(), "nexthink")
  implicit val executionContext = actorSystem.executionContext

  val routes =
    path("deploy") {
      post {
        entity(as[List[ServiceDeployment]]) { serviceDeployment =>
          actorSystem ! TaskManagerRequestProtocol.Deploy(serviceDeployment)
          complete(StatusCodes.OK)
        }
      }
    } ~
      path("check") {
        get {
          actorSystem ! TaskManagerRequestProtocol.CheckHealth
          complete(StatusCodes.OK)
        }
      }

  val bindingFuture = Http().newServerAt("localhost", 8080).bind(routes)

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ => actorSystem.terminate()) // and shutdown when done
}
