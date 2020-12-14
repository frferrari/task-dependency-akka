package com.fferrari

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import com.fferrari.actor.TaskManagerActor
import com.fferrari.actor.protocol.{TaskManagerRequestProtocol, TaskManagerResponseProtocol}
import com.fferrari.model.{ServiceDeployment, ServiceDeploymentJsonProtocol}

import scala.concurrent.duration.DurationInt
import scala.io.StdIn
import scala.util.{Failure, Success}

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
          onComplete {
            implicit val timeout: Timeout = 3.seconds
            actorSystem.ask(ref => TaskManagerRequestProtocol.Deploy(serviceDeployment, ref))
          } {
            case Success(TaskManagerResponseProtocol.DeploymentSuccessful) =>
              complete(StatusCodes.OK)

            case Success(_) =>
              complete(
                StatusCodes.InternalServerError,
                HttpEntity(
                  ContentTypes.`application/json`,
                  """{ "status": "UNKNOWN", "reason": "Unknown deployment status" }"""
                )
              )

            case Failure(_) =>
              complete(StatusCodes.InternalServerError)
          }
        }
      }
    } ~
      path("check") {
        get {
          onComplete {
            implicit val timeout: Timeout = 3.seconds
            actorSystem.ask(ref => TaskManagerRequestProtocol.CheckHealth(ref))
          } {
            case Success(TaskManagerResponseProtocol.HealthStatus(true)) =>
              complete(
                StatusCodes.OK,
                HttpEntity(
                  ContentTypes.`application/json`,
                  """{ "status": "HEALTHY" }"""
                )
              )

            case Success(TaskManagerResponseProtocol.HealthStatus(false)) =>
              complete(
                StatusCodes.InternalServerError,
                HttpEntity(
                  ContentTypes.`application/json`,
                  """{ "status": "UNHEALTHY" }"""
                )
              )

            case _ =>
              complete(
                StatusCodes.InternalServerError,
                HttpEntity(
                  ContentTypes.`application/json`,
                  """{ "status": "UNKNOWN" }"""
                )
              )
          }
        }
      }

  val bindingFuture = Http().newServerAt("localhost", 8080).bind(routes)

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ => actorSystem.terminate()) // and shutdown when done
}
