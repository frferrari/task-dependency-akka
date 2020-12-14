package com.fferrari.actor.protocol

import com.fferrari.model.ServiceDeployment

object TaskManagerRequestProtocol {

  trait Request

  final case class Deploy(services: List[ServiceDeployment]) extends Request

  final case object CheckHealth extends Request

  final case object HealthCheckTimeout extends Request

}
