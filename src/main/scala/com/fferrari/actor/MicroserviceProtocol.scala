package com.fferrari.actor

import akka.actor.typed.ActorRef
import akka.actor.typed.receptionist.Receptionist
import com.fferrari.actor.ServiceManagerProtocol.ServiceManagerCommand

object MicroserviceProtocol {
  sealed trait MicroserviceCommand

  final case class EntryPointHealthCheck(replyTo: ActorRef[ServiceManagerCommand]) extends MicroserviceCommand
  final case class HealthCheck(replyTo: ActorRef[MicroserviceCommand]) extends MicroserviceCommand
  final case object CheckDependencies extends MicroserviceCommand
  final case class HealthyService(healthy: Boolean) extends MicroserviceCommand

  final case class ListingResponse(listing: Receptionist.Listing) extends MicroserviceCommand

  final case class Healthy(name: String) extends MicroserviceCommand
  final case class Unhealthy(name: String) extends MicroserviceCommand
}
