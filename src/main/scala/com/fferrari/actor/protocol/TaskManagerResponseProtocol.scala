package com.fferrari.actor.protocol

import com.fferrari.actor.TaskProtocol

object TaskManagerResponseProtocol {

  trait Response

  final case object TaskIsHealthy extends Response

  final case class WrappedTaskResponse(response: TaskProtocol.Response) extends TaskManagerRequestProtocol.Request

}
