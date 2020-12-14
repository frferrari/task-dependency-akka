package com.fferrari.actor

import akka.actor.typed.receptionist.Receptionist.{Find, Listing}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.actor.typed.{Behavior, SupervisorStrategy}
import akka.util.Timeout
import com.fferrari.actor.MicroserviceProtocol.{Healthy, MicroserviceCommand, Unhealthy}
import com.fferrari.actor.ServiceManagerProtocol.{Deploy, HealthCheck, ServiceManagerCommand}
import com.fferrari.model.ServiceDeployment

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/*
object ServiceManagerActor {

  def apply(): Behavior[ServiceManagerCommand] = processingDeployments(List.empty[ServiceDeployment])

  def processingDeployments(deployed: List[ServiceDeployment]): Behavior[ServiceManagerCommand] = Behaviors.receive {
    case (context, Deploy(deployments)) if deployed.isEmpty =>
      println(s"Deploying $deployments")

      deployments.foreach { deployment =>
        val actorName = deployment.serviceName
        val pool = Routers.pool(poolSize = deployment.replicas)(
          Behaviors.supervise(MicroserviceActor(deployment.entryPoint, deployment.dependencies)).onFailure[Exception](SupervisorStrategy.restart))
        val router = context.spawn(pool, actorName)
        val microserviceKey = ServiceKey[MicroserviceCommand](actorName)
        context.system.receptionist ! Receptionist.Register(microserviceKey, router)
      }
      processingDeployments(deployments)

    case (context, Deploy(deployments)) if deployed.nonEmpty =>
      println(s"Already deployed, Deploying $deployments")
      processingDeployments(deployments)

    case (context, HealthCheck) if deployed.nonEmpty =>
      deployed
        .find(_.entryPoint == true)
        .foreach { entryPoint =>
          implicit val timeout: Timeout = 2.seconds
          val entryPointKey = ServiceKey[MicroserviceCommand](entryPoint.serviceName)
          context.ask(context.system.receptionist, Find(entryPointKey)) {
            case Success(listing: Listing) =>
              ServiceManagerProtocol.Healthy
            case Failure(_) =>
              Unhealthy(entryPoint.serviceName)
          }
        }
      Behaviors.same
  }

  def processingHealthCheck = Behaviors.receive {
    case (context, Healthy) =>
      Behaviors.same
  }
}
*/