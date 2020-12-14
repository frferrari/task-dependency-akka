package com.fferrari.actor.protocol

object TaskManagerResponseProtocol {

  trait Response

  final case object TaskIsHealthy extends Response

  final case class WrappedTaskResponse(response: TaskResponseProtocol.Response) extends TaskManagerRequestProtocol.Request

}
