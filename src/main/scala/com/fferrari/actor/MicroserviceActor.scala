package com.fferrari.actor

import akka.actor.typed.receptionist.Receptionist.{Find, Listing}
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import com.fferrari.actor.MicroserviceProtocol._
import com.fferrari.model.Microservice

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/*
object MicroserviceActor extends Microservice {

  var isEntryPoint: Boolean = false
  var dependencies: List[String] = List.empty[String]

  def apply(entryPoint: Boolean, deps: List[String]): Behavior[MicroserviceCommand] = Behaviors.setup { context =>
    isEntryPoint = entryPoint
    dependencies = deps

    awaitingHealthCheckRequest
  }

  def awaitingHealthCheckRequest: Behavior[MicroserviceCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case HealthCheck(replyTo) if dependencies.nonEmpty =>
          // This service has dependencies, check if they are healthy
          context.self ! CheckDependencies
          healthChecker(replyTo, dependencies.map(dependency => dependency -> Option.empty[Boolean]).toMap)

        case HealthCheck(replyTo) if dependencies.isEmpty =>
          // This service has no dependency, it is healthy
          replyTo ! HealthyService(true)
          Behaviors.same
      }
    }

  def healthChecker(replyTo: ActorRef[MicroserviceCommand], healthy: Map[String, Option[Boolean]]): Behavior[MicroserviceCommand] =
    Behaviors.receive { (context, message) =>
      message match {
        case CheckDependencies =>
          implicit val timeout: Timeout = 1.second
          dependencies.foreach { dependency =>
            val microserviceKey = ServiceKey[MicroserviceCommand](dependency)
            context.ask(context.system.receptionist, Find(microserviceKey)) {
              case Success(listing: Listing) =>
                Healthy(dependency)
              case Failure(_) =>
                Unhealthy(dependency)
            }
          }
          Behaviors.same

        case Healthy(name) =>
          val newHealthy = healthy + (name -> Some(true))

          allChecked(newHealthy) match {
            case Some(health) =>
              replyTo ! HealthyService(health)
              awaitingHealthCheckRequest
            case None =>
              healthChecker(replyTo, newHealthy)
          }

        case Unhealthy(name) =>
          val newHealthy = healthy + (name -> Some(false))
          healthChecker(replyTo, newHealthy)
      }
    }

  def allChecked(healthy: Map[String, Option[Boolean]]): Option[Boolean] = {
    if (healthy.values.exists(_.isEmpty))
      None
    else {
      Some(healthy
        .values
        .flatten
        .reduce(_ & _))
    }
  }
}
*/