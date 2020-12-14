package com.fferrari.actor.protocol

object TaskManagerResponseProtocol {

  trait Response

  final case class HealthStatus(isHealthy: Boolean) extends Response

  final case class WrappedTaskResponse(response: TaskResponseProtocol.Response) extends TaskManagerRequestProtocol.Request

}
