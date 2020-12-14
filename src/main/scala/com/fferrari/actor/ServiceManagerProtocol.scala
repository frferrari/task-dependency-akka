package com.fferrari.actor

import com.fferrari.model.ServiceDeployment

object ServiceManagerProtocol {
  sealed trait ServiceManagerCommand

  final case class Deploy(deployments: List[ServiceDeployment]) extends ServiceManagerCommand
  final case object HealthCheck extends ServiceManagerCommand
  final case object Healthy extends ServiceManagerCommand
  final case object Unhealthy extends ServiceManagerCommand
}
