package com.fferrari.actor.protocol

import akka.actor.typed.ActorRef

object TaskRequestProtocol {

  sealed trait Request

  final case class CheckHealth(replyTo: ActorRef[TaskResponseProtocol.Response]) extends Request

  final case object Stop extends Request
}
