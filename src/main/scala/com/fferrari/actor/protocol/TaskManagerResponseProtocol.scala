package com.fferrari.actor.protocol

object TaskManagerResponseProtocol {

  trait Response

  final case object DeploymentSuccessful extends Response
  final case object DeploymentError extends Response

  final case class HealthStatus(isHealthy: Boolean) extends Response

  final case object ServiceIsNotDeployed extends Response

  final case class WrappedTaskResponse(response: TaskResponseProtocol.Response) extends TaskManagerRequestProtocol.Request

}
