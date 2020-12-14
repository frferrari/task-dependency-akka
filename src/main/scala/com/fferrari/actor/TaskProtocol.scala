package com.fferrari.actor

import akka.actor.typed.ActorRef

object TaskProtocol {
  sealed trait Request
  final case class CheckHealth(replyTo: ActorRef[Response]) extends Request

  sealed trait Response
  final case object TaskIsHealthy extends Response
}
