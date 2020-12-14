package com.fferrari.actor.protocol

object TaskResponseProtocol {

  sealed trait Response

  final case object TaskIsHealthy extends Response

}
