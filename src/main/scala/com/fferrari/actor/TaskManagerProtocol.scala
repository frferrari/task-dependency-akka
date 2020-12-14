package com.fferrari.actor

import com.fferrari.model.ServiceDeployment

object TaskManagerProtocol {

  sealed trait Request

  final case class Deploy(services: List[ServiceDeployment]) extends Request

  final case object CheckHealth extends Request

  sealed trait Response

  final case object TaskIsHealthy extends Response

  final case class WrappedTaskResponse(response: TaskProtocol.Response) extends TaskManagerProtocol.Request

}
