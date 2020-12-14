package com.fferrari.actor.protocol

import akka.actor.typed.ActorRef
import com.fferrari.model.ServiceDeployment

object TaskManagerRequestProtocol {

  trait Request

  final case class Deploy(services: List[ServiceDeployment]) extends Request

  final case class CheckHealth(replyTo: ActorRef[TaskManagerResponseProtocol.Response]) extends Request

  final case object HealthCheckTimeout extends Request

}
