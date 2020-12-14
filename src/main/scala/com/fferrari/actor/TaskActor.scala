package com.fferrari.actor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object TaskActor {
  def apply(): Behavior[TaskProtocol.Request] = Behaviors.setup[TaskProtocol.Request] { context =>
    context.log.info(s"Starting Task ${context.self.path}")
    Behaviors.receiveMessage[TaskProtocol.Request] {
      case TaskProtocol.CheckHealth(replyTo) =>
        context.log.info(s"Handling CheckHealth request for Task ${context.self.path}")
        replyTo ! TaskProtocol.TaskIsHealthy
        Behaviors.same
    }
  }
}
